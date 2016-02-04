from __future__ import absolute_import, division

import sys
import os
import io
import tempfile
import errno

if sys.version_info < (3,):
    from urllib2 import urlopen
    import urllib
    _httpresponse = urllib.addinfourl
    _iowrappers = (file, io.IOBase)
else:
    from urllib.request import urlopen
    import http.client
    _httpresponse = http.client.HTTPResponse
    _iowrappers = (io.IOBase, )

from . import stats
from . import expression
from . import group


def progress_bar_milestones(count, iterations=100):
    return set([int(i * count / iterations) for i in range(iterations)])


def copyfileobj(fsrc, fdst, buffer=2 ** 15, totalsize=None, progress=None):
    """
    Copy contents from a file-like object `fsrc` to a file-like `fdst`.

    Like shutil.copyfileobj but with progress reporting.

    Parameters
    ----------
    src : file-like object
        Source file object open for reading.
    dst : file-like object
        Destination file object open for writing in binary mode.
    buffer : buffer size
        Buffer size
    totalsize : int optional
        Total `fsrc` contents size if available.
    progress : (int, int) -> None, optional
        An optional progress callback function. Will be called
        periodically with `(transfered, total)` bytes count. `total`
        will be `-1` if the total contents size cannot be
        determined beforehand.
    """
    if totalsize is None:
        try:
            totalsize = sniff_size(fsrc)
        except (IOError, OSError):
            pass

    count = 0
    while True:
        data = fsrc.read(buffer)
        if not data:
            break

        fdst.write(data)
        count += len(data)

        if progress is not None:
            progress(count, totalsize if totalsize is not None else -1)

    if totalsize is None:
        progress(count, count)

    return count


def retrieve_url(url, dstobj, timeout=30, progress=None):
    """
    Retrieve contents at `url` writing it to an open file-like `destobj`.

    Parameters
    ----------
    url : str
        The source url.
    destobj : file-like object
        An file-like object opened for writing in binary mode.
    timeout : int, optional
        Connection timeout
    progress : (int, int) -> None optional
        An optional progress callback function. Will be called
        periodically with `(transfered, total)` bytes count. `total`
        will be `-1` if the total contents size cannot be
        determined beforehand.
    """
    with urlopen(url, timeout=timeout) as stream:
        length = content_length(stream)
        copyfileobj(stream, dstobj, totalsize=length, progress=progress)


def download_url(url, localpath, timeout=30, progress=None):
    """
    Download the contents at `url` to a file in a local file system.

    Note: A temporary file in the same directory as `localpath` is created
    into which the contents are written and then moved into place.

    Parameters
    ----------
    url : str
        The source url.
    localpath : str
        A destination path in the local file system.
    timeout : int, optional
        Connection timeout
    progress : (int, int) -> None
        A optional progress callback function Will be called
        periodically with `(transfered, total)` bytes count. `total`
        will be `-1` if the total contents size cannot be
        determined beforehand.
    """
    dirname, basename = os.path.split(localpath)
    if not basename:
        raise ValueError

    temp = tempfile.NamedTemporaryFile(
       prefix=basename + "-", dir=dirname, delete=False)
    os.chmod(temp.name, 0o644)
    try:
        retrieve_url(url, temp, timeout=timeout, progress=progress)
    except BaseException:
        try:
            temp.close()
            os.remove(temp.name)
        except OSError:
            pass
        raise
    else:
        temp.close()
        replace(temp.name, localpath)


def content_length(response):
    length = response.headers.get("content-length", None)
    if length is None:
        length = -1
    else:
        try:
            length = int(length)
        except ValueError:
            length = -1
    return length


def sniff_size(fileobj):
    # check for HTTPResponse first (it is also an io.IOBase)
    if isinstance(fileobj, _httpresponse):
        return content_length(fileobj)
    elif isinstance(fileobj, _iowrappers):
        try:
            return os.fstat(fileobj.fileno()).st_size
        except (IOError, OSError):
            return None
    return None

if sys.version_info < (3, 2):
    def makedirs(path, mode=0o777, exist_ok=False):
        """
        Like `os.makedirs` in Python 3.2
        """
        try:
            os.makedirs(path, mode)
        except OSError as e:
            if exist_ok and e.errno == errno.EEXIST and os.path.isdir(path):
                return
            else:
                raise
if sys.version_info < (3, 3) and os.name == "nt":
    def replace(srcpath, dstpath):
        try:
            os.rename(srcpath, dstpath)
        except OSError as e:
            if e.errno == errno.EEXIST:
                os.remove(dstpath)
                os.rename(srcpath, dstpath)
            else:
                raise
elif sys.version_info < (3, 3):
    replace = os.rename
else:
    replace = os.replace


def isdirname(path):
    """
    Does a path name a directory, i.e. ends with a path component separator.

    Note: Does not check that path actually exists and is a dir.
    """
    return path.endswith(os.path.sep) or \
           (os.path.altsep and path.endswith(os.path.altsep))
