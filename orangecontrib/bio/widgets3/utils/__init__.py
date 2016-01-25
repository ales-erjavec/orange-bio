from numbers import Real
from functools import partial, wraps

import numpy


@partial(numpy.vectorize, otypes=[bool])
def isnan_fail(a):
    return isinstance(a, Real) and numpy.isnan(a)


def pyufunc(nin=1, nout=1):
    def decorator(func):
        vfunc = numpy.frompyfunc(func, nin, nout)
        return vfunc
    return decorator


@pyufunc(nin=1, nout=1)
def isnan_obj(a):
    return isinstance(a, Real) and numpy.isnan(a)


def isunknown(var, a):
    """
    Parameters
    ----------
    var : Orange.data.Variable
    a : array-like

    Returns
    -------
    mask : numpy.ndarray or bool
        A bool or a bool ndarray mask matrix with True where the element
        in `a` is unknown.
    """
    if var.is_discrete or var.is_continuous:
        return numpy.isnan(a)
    else:
        if isinstance(var.Unknown, Real) and numpy.isnan(var.Unknown):
            return isnan_obj(a).astype(bool, copy=False)
        else:
            return numpy.equal(a, var.Unknown)


def gene_names_from_column(table, column):
    """
    Parameters
    ----------
    table : Orange.data.Table
    column : Orange.data.Variable

    Returns
    -------
    genes : List[str]
    """
    var = table.domain[column]
    data, _ = table.get_column_view(var)
    data = data[~isunknown(var, data)]
    return [var.str_val(v) for v in data]
