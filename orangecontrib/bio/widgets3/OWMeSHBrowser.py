import sys
import operator

from functools import reduce


from PyQt4 import QtGui, QtCore
from PyQt4.QtCore import Qt

import Orange.data

from Orange.widgets import widget, gui, settings
from Orange.widgets.utils import concurrent, itemmodels

from ..widgets.utils.download import EnsureDownloaded

from .. import geneset
from ..utils import serverfiles

from .OWSetEnrichment import set_enrichment


class OWMeSHBrowser(widget.OWWidget):
    name = "MeSH Browser"
    description = "Browse MeSH (Medical Subject Headings)"
    icon = "../widgets/icons/MeSHBrowser.svg"
    priority = 2040

    inputs = [("Query data", Orange.data.Table, "set_data", widget.Default),
              ("Reference data", Orange.data.Table, "set_reference_data")]

    outputs = [("Selected data", Orange.data.Table)]

    settingsHandler = settings.DomainContextHandler()

    pcid_var_index = settings.ContextSetting(-1)

    max_pvalue = settings.Setting(0.05)
    min_count = settings.Setting(5)

    use_ref = settings.Setting(False)
    auto_commit = settings.Setting(True)

    # Widget state
    Ready, Initializing, Running = 1, 2, 3

    Header = ["Name", "Description", "Num. query", "Num. reference",
              "P-value", "Enrichment"]

    def __init__(self, parent=None):
        super().__init__(parent)

        #: Input query dataset
        self.data = None
        #: Input reference dataset
        self.reference_data = None
        #: Loaded mesh DAG structure
        self.mesh = None
        # Enrichment results (when available)
        self.results = None

        self.__state = 0
        self.__terms = None
        self.__sets = None

        box = gui.widgetBox(self.controlArea, "Info")

        self.infoa = gui.label(box, self, "No reference data.")
        self.infob = gui.label(box, self, "No query data.")
        self.ratio = gui.label(box, self, "")

        box = gui.widgetBox(self.controlArea, "Entity Ids")
        self.pcid_var_cb = gui.comboBox(box, self, "pcid_var_index")
        self.variables_model = itemmodels.VariableListModel(parent=self)
        self.pcid_var_cb.setModel(self.variables_model)

        form = QtGui.QFormLayout(
            formAlignment=Qt.AlignLeft,
            labelAlignment=Qt.AlignLeft,
            fieldGrowthPolicy=QtGui.QFormLayout.AllNonFixedFieldsGrow)

        gui.widgetBox(self.controlArea, "Options", orientation=form)
        self.maxp = QtGui.QDoubleSpinBox(
            self, minimum=0., maximum=1., value=self.max_pvalue)
        self.maxp.valueChanged[float].connect(self._set_max_pvalue)
        form.addRow("Max p-value:", self.maxp)

        self.minc = QtGui.QSpinBox(
            self, minimum=0, maximum=100, value=self.min_count)
        self.minc.valueChanged[int].connect(self._set_min_count)
        form.addRow("Min. frequency:", self.minc)

        self.splitter = QtGui.QSplitter(Qt.Vertical, self.mainArea)
        self.mainArea.layout().addWidget(self.splitter)

        # a default item model displayed when 'empty'
        self.__empty_model = QtGui.QStandardItemModel(self)
        self.__empty_model.setHorizontalHeaderLabels(OWMeSHBrowser.Header)

        # tree view
        self.mesh_tree_view = QtGui.QTreeView(
            rootIsDecorated=True,
            selectionMode=QtGui.QTreeView.MultiSelection,
            sortingEnabled=True)
        self.mesh_tree_view.setModel(self.__empty_model)

        # table of significant mesh terms
        self.mesh_table_view = QtGui.QTableView(
            selectionMode=QtGui.QTableView.MultiSelection,
            selectionBehavior=QtGui.QTableView.SelectRows,
            sortingEnabled=True)

        self.mesh_table_view.setModel(self.__empty_model)

        self.splitter.addWidget(self.mesh_tree_view)
        self.splitter.addWidget(self.mesh_table_view)

        self.controlArea.layout().addStretch()
        gui.auto_commit(self.controlArea, self, "auto_commit", "Commit")

        self._executor = concurrent.ThreadExecutor()
        sflist = [("MeSH", "mesh-ontology.dat"),
#                   (mesh.DOMAIN, mesh.MESH_FILENAME),
                  (geneset.sfdomain,
                   geneset.filename(("MESH", "Chemicals"), None))]
        self.__state = OWMeSHBrowser.Initializing
        self.__init_task = EnsureDownloaded(sflist)
        self.__init_task.finished.connect(self.__init_finish)
        self.__init_f = self._executor.submit(self.__init_task)

    def __init_finish(self):
        self.__state = OWMeSHBrowser.Ready
        if self.__init_f.exception() is not None:
            exc = self.__init_f.exception()
            sys.excepthook(type(exc), exc, exc.__traceback__)

    def sizeHint(self):
        return QtCore.QSize(960, 600)

    def set_data(self, data):
        """Set the query input dataset."""
        if self.__state == OWMeSHBrowser.Initializing:
            self.__init_f.result()

        self.clear()
        self.data = data

        if data is not None:
            variables = data.domain.variables + data.domain.metas
            variables = [var for var in variables
                         if isinstance(var, Orange.data.StringVariable)]

            self.variables_model[:] = variables

        self._invalidate()

    def set_reference_data(self, data):
        self.reference_data = data
        if self.use_ref:
            self._invalidate()

    def clear(self):
        self.results = None
        model = self.mesh_table_view.model()
        if model is not None and model is not self.__empty_model:
            model.deleteLater()
        self.mesh_table_view.setModel(self.__empty_model)

        model = self.mesh_tree_view.model()
        if model is not None and model is not self.__empty_model:
            model.deleteLater()
        self.mesh_tree_view.setModel(self.__empty_model)

    def _clear_results(self):
        self.results = None
        self.__header_state = self.mesh_table_view.horizontalHeader().saveState()

    def _set_results(self, results):
        if self.__terms is None:
            self.__terms = load_mesh(
                serverfiles.localpath_download("MeSH", "mesh-ontology.dat"))
        terms = self.__terms

        term_by_name = {term.name: term for term in terms}
        term_by_path = {tuple(tid.split(".")): term
                        for term in terms for tid in term.ids}

        def item(display, tooltip=None, link=None, user=None):
            item = QtGui.QStandardItem()
            item.setData(display, Qt.DisplayRole)
            if tooltip is not None:
                item.setToolTip(tooltip)
            if link is not None:
                item.setData(link, gui.LinkRole)
            if user is not None:
                item.setData(user, Qt.UserRole)
            return item

        model = QtGui.QStandardItemModel()
        model.setHorizontalHeaderLabels(OWMeSHBrowser.Header)
        for gs, enrich in results:
            term = term_by_name.get(gs.name, None)
            if term is None:
                continue

            row = [
                item(gs.name, link=gs.link),
                item(term.description, tooltip=term.description),
                item(len(enrich.query_mapped)),
                item(len(enrich.reference_mapped)),
                item(enrich.p_value),
                item(enrich.enrichment_score),
            ]

            model.appendRow(row)

        self.mesh_table_view.setModel(model)

        model = QtGui.QStandardItemModel()
        item_by_path = {(): [item(None)]}

        for gs, enrich in results:
            term = term_by_name.get(gs.name, None)
            if term is None:
                continue

            for path in term.ids:
                path = tuple(path.split("."))
                row = [
                    item(term.name, link=gs.link, user=(gs, term, enrich)),
                    item(term.description, tooltip=term.description),
                    item(len(enrich.query_mapped)),
                    item(len(enrich.reference_mapped)),
                    item(enrich.p_value),
                    item(enrich.enrichment_score)
                ]
                item_by_path[path] = row

        for path, row in list(item_by_path.items()):
            if len(path) == 0:
                continue

            prefix = ()
            parent = item_by_path[prefix][0]
            assert len(path) > 0
            for p in path[:-1]:
                prefix = prefix + (p, )
                parent = item_by_path.get(prefix)
                if parent is None:
                    term = term_by_path[prefix]
                    row_ = [item(term.name)]
                    item_by_path[prefix] = row_
                    item_by_path[prefix[:-1]][0].appendRow(row_)
                    parent = row_[0]
                else:
                    parent = parent[0]

            assert parent is not None
            parent.appendRow(row)

        root, = item_by_path[()]
        toprows = [root.takeRow(i) for i in reversed(range(root.rowCount()))]
        toprows = reversed(toprows)
        for row in toprows:
            model.appendRow(row)
        model.setHorizontalHeaderLabels(OWMeSHBrowser.Header)
        self.mesh_tree_view.setModel(model)
        self.mesh_tree_view.resizeColumnToContents(0)

        self.mesh_table_view.selectionModel().selectionChanged.connect(
            self.__on_table_selection_changed)
        self.mesh_tree_view.selectionModel().selectionChanged.connect(
            self.__on_tree_selection_changed)

    def _invalidate(self):
        self._clear_results()
        self.results = None

    def handleNewSignals(self):
        if self.results is None and self.data is not None:
            self._update_enrichment()

    def _update_enrichment(self):
        if self.data is None or self.pcid_var() is None:
            return

        if self.__sets is None:
            self.__sets = geneset.collections((('MeSH', 'Chemicals'), None))
        sets = self.__sets

        query = self._ids_from_data(self.data, self.pcid_var())

        if self.reference_data is not None and self.use_ref:
            reference = self._ids_from_data(
                self.reference_data, self.pcid_var()())
        else:
            reference = reduce(
                operator.ior, (set(gs.genes) for gs in sets), set())

        results = [(gs, set_enrichment(gs.genes, reference, query))
                   for gs in sets]
        results = [(gs, res) for gs, res in results if len(res.query_mapped)]

        self._set_results(results)

    def _ids_from_data(self, table, var):
        column, _ = table.get_column_view(var)
        return set(map(var.str_val, column))

    def pcid_var(self):
        if 0 <= self.pcid_var_index < len(self.variables_model):
            return self.variables_model[self.pcid_var_index]
        else:
            return None

    def _set_min_count(self, value):
        self.min_count = value

    def _set_max_pvalue(self, value):
        self.max_pvalue = value

    def __on_table_selection_changed(self):
        if self.results is None:
            return

    def __on_tree_selection_changed(self):
        if self.results is None:
            return

    def commit(self):
        data = None
        if self.results is not None:
            selection = self.mesh_tree_view.selectionModel().selectedRows(0)
            items = [index.data(Qt.UserRole) for index in selection]
#             indices = sorted(item.index for item in items)
            data = self.data[indices]
        self.send("Selected data", data)


import io
import csv
from types import SimpleNamespace as namespace
from collections import namedtuple


def load_mesh(path):
    terms = []
    mesh_file = io.open(path, "r")
    for name, ids, description in csv.reader(mesh_file, delimiter="\t"):
        ids = ids.split(";")
        terms.append(namespace(name=name, ids=ids, description=description))
    return terms


def load_cid_annoations(path):
    annots = []
    annot_f = io.open(path, "r")
    for cid, name in csv.reader(annot_f, delimiter=";"):
        annots.append(namespace(cid=cid, name=name))
    return annots

MeSHNode = namedtuple("MeSHNode", ["term", "branches"])


def mesh_tree(terms):
    def insert(node, key, term):
        if len(key) == 0:
            return (node[0], node[1] + (term,))
        else:
            key_head, *key_rest = key

            if any(key_head == ch[0] for ch in node[1]):
                return (node[0], tuple(insert(ch, key_rest, term) if key_head == ch[0] else ch
                                       for ch in node[1]))
            else:
                return (node[0], node[1] + insert((key_head, ()), key_rest, term))

    root = ('root', ())

    for term in terms:
        for meshid in term.ids:
            root = insert(root, meshid.split("."), term)

    return root


def test_main(argv=sys.argv):
    if len(argv) > 1:
        filename = argv[1]
    else:
        filename = "chemogenomics.tab"
    data = Orange.data.Table(filename)

    app = QtGui.QApplication(argv)
    w = OWMeSHBrowser()
    w.set_data(data)
    w.show()
    w.raise_()
    w.handleNewSignals()
    r = app.exec_()
    w.saveSettings()
    w.set_data(None)
    return r

if __name__ == "__main__":
    sys.exit(test_main())
