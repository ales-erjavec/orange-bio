##!interval=7
##!contact=ales.erjavec@fri.uni-lj.si
from __future__ import print_function

import urllib2
import re
import cPickle
import gzip
import io

from datetime import datetime
from collections import defaultdict

from common import *

from orangecontrib.bio import go, taxonomy, gene, utils


tmp_path = os.path.join(environ.buffer_dir, "tmp_GO")
utils.makedirs(tmp_path, exist_ok=True)

DATE_FMT_1 = "%Y-%m-%d %H:%M:%S.%f"
DATE_FMT_2 = "%Y-%m-%d %H:%M:%S"


fileslist = sf_server.listfiles("GO")

def info_date_time_parse(time):
    """
    Parse a "datetime" field from the sf info record into a datetime.datetime.
    """
    try:
        return datetime.strptime(time, DATE_FMT_1)
    except ValueError:
        return datetime.strptime(time, DATE_FMT_2)


def http_last_modified(url):
    """
    Retrieve a "last-modified" time for the url as a datetime.datetime object.
    """
    stream = urllib2.urlopen(url)
    return datetime.strptime(stream.headers.get("Last-Modified"),
                             "%a, %d %b %Y %H:%M:%S %Z")


def list_available_organisms():
    """
    Return a list of all available GO organism codes.
    """
    source = urllib2.urlopen("http://www.geneontology.org/gene-associations/").read()
    codes = re.findall("gene_association\.([a-zA-z0-9_]+?)\.gz", source)
    return sorted(set(codes))


def sf_org_mtime(org_code):
    fname = "gene_association.{}".format(org_code)
    if fname in fileslist:
        info = sf_server.info("GO", fname)
        return info_date_time_parse(info["datetime"])
    else:
        return datetime.fromtimestamp(0)


def web_org_mtime(org_code):
    return http_last_modified(
        "http://www.geneontology.org/gene-associations/gene_association.{}.gz"
        .format(org_code))


def sf_ontology_mtime():
    fname = "gene_ontology_edit.obo"
    if fname in fileslist:
        info = sf_server.info("GO", fname)
        return info_date_time_parse(info["datetime"])
    else:
        return datetime.fromtimestamp(0)


def web_ontology_mtime():
    return http_last_modified(
        "http://www.geneontology.org/ontology/gene_ontology.obo")


def gzip_file(srcfname, dstfname=None):
    if dstfname is None:
        dstfname = srcfname + ".gz"

    with open(srcfname, "rb") as file:
        with gzip.open(dstfname, "wb") as gzfile:
            utils.copyfileobj(file, gzfile)


def gz_uncompressed_size(filename):
    """
    WARNING: The reported size is modulo 2 ** 32 i.e. only correct for files
    smaller than ~4GB
    """
    import struct
    with open(filename, "rb") as f:
        # the size (modulo 2 ** 32) is encoded in the last 4 bytes
        # little-endian order (RFC 1952)
        f.seek(-4, io.SEEK_END)
        size, = struct.unpack("<I", f.read(4))
        return size


if web_ontology_mtime() > sf_ontology_mtime():
    print("donwloading ontology")
    filename = os.path.join(tmp_path, "gene_ontology_edit.obo")

    go.Ontology.download(filename)

    # load the ontology to test it
    o = go.Ontology(filename)
    del o
    size = os.stat(filename).st_size
    # gzip it
    gzip_file(filename)

    ##upload the ontology
    print("Uploading gene_ontology_edit.obo")
    sf_server.upload(
        "GO", "gene_ontology_edit.obo", filename + ".gz",
        title="Gene Ontology (GO)",
        tags=["gene", "ontology", "GO", "essential", "#compression:gz",
              "#uncompressed:%i" % size,
              "#version:%i" % go.Ontology.version]
    )
    sf_server.unprotect("GO", "gene_ontology_edit.obo")


orgMap = {"352472": "44689", "562": "83333", "3055": None,
          "7955": None, "11103": None, "2104": None, "4754":
          None, "31033": None, "8355": None, "4577": None}

commonOrgs = dict([(go.from_taxid(id), id)
                   for id in taxonomy.common_taxids()
                   if go.from_taxid(id) != None])

essentialOrgs = [go.from_taxid(id) for id in taxonomy.essential_taxids()]

exclude = ["goa_uniprot", "goa_pdb", "GeneDB_tsetse", "reactome",
           "goa_zebrafish", "goa_rat", "goa_mouse"]

updatedTaxonomy = defaultdict(set)


for org in list_available_organisms():

    if org in exclude or org not in commonOrgs:
        continue

    print("Query", org)

    if 1 and web_org_mtime(org) <= sf_org_mtime(org):
        # Skip update
        continue

    print("Updating", org)

    filename = os.path.join(tmp_path, "gene_association.{}.gz".format(org))
    go.Annotations.download(org, filename)
    # Load the annotations to test them and collect all taxon ids from them
    print(filename)
    a = go.Annotations(filename, genematcher=gene.GMDirect())
    taxons = set([ann.Taxon for ann in a.annotations])
    # exclude taxons with cardinality 2
    taxons = [tax for tax in taxons if "|" not in tax]
    for tax in taxons:
        taxid = tax.split(":", 1)[-1]
        updatedTaxonomy[taxid].add(org)
    del a

    orgName = taxonomy.name(commonOrgs[org])
    taxid = taxonomy.taxname_to_taxid(orgName)

    print("Uploading", "gene_association.{}.gz".format(org))
    sf_server.upload(
        "GO", "gene_association.{}".format(org), filename,
        title="GO Annotations for " + orgName,
        tags=["gene", "annotation", "ontology", "GO", orgName,
              "#uncompressed:%i" % gz_uncompressed_size(filename),
              "#organism:" + orgName,
              "#version:%i" % go.Annotations.version] +
             (["essential"] if org in essentialOrgs else []) +
             taxonomy.shortname(taxid)
    )
    sf_server.unprotect("GO", "gene_association.{}".format(org))

try:
    tax = cPickle.load(open(sf_local.localpath_download("GO", "taxonomy.pickle"), "rb"))
except Exception:
    tax = {}

# Upload taxonomy if any differences in the updated taxonomy
if any(tax.get(key, set()) != updatedTaxonomy.get(key, set())
       for key in set(updatedTaxonomy)):
    tax.update(updatedTaxonomy)
    cPickle.dump(tax, open(os.path.join(tmp_path, "taxonomy.pickle"), "wb"),
                 protocol=0)
    print("Uploading", "taxonomy.pickle")
    sf_server.upload(
        "GO", "taxonomy.pickle", os.path.join(tmp_path, "taxonomy.pickle"),
        title="GO taxon IDs",
        tags=["GO", "taxon", "organism", "essential",
              "#version:%i" % go.Taxonomy.version])
    sf_server.unprotect("GO", "taxonomy.pickle")
