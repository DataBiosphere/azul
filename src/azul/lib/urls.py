import urllib.parse

from furl import (
    furl,
)

from azul.lib import (
    mutable_furl,
)


def _normalize_path(path: str) -> str:
    if path:
        resolved = urllib.parse.urljoin('http://x/', path)
        normalized = urllib.parse.urlparse(resolved).path
        if not path.startswith('/'):
            normalized = normalized.lstrip('/')
        return normalized
    else:
        return path


def normalize_url(url: furl) -> mutable_furl:
    """
    Return a normalized copy of the given URL. The caller can further mutate the
    copy without affecting the original.

    >>> def f(url):
    ...     return str(normalize_url(furl(url)))

    Scheme and host are lowercased by furl:

    >>> f('HTTP://X')
    'http://x'

    Default ports are removed by furl:

    >>> f('http://x:80')
    'http://x'

    Dot segments in the path are resolved per RFC 3986:

    >>> f('http://x/a/b/../c')
    'http://x/a/c'

    >>> f('http://x/a/./b')
    'http://x/a/b'

    Trailing slashes are preserved:

    >>> f('http://x/a/')
    'http://x/a/'

    An empty path is preserved:

    >>> f('http://x')
    'http://x'

    Query parameters are sorted:

    >>> f('http://x?b=2&a=1')
    'http://x?a=1&b=2'

    Duplicate query parameters are preserved and sorted by value:

    >>> f('http://x?a=2&a=1')
    'http://x?a=1&a=2'

    Fragment query parameters are sorted:

    >>> f('http://x#s?b=2&a=1')
    'http://x#s?a=1&b=2'

    The fragment path is not normalized (it is opaque per RFC 3986):

    >>> f('http://x#a/b/../c')
    'http://x#a/b/../c'

    A fragment without query parameters is unchanged:

    >>> f('http://x#s')
    'http://x#s'
    """
    url = url.copy()
    url.set(path=_normalize_path(str(url.path)))
    url.set(query=sorted(url.query.params.allitems()))
    if str(url.fragment):
        url.fragment.set(args=sorted(url.fragment.query.params.allitems()))
    return url
