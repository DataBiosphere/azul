"""
Bare-bones typing stubs for furl. The distinction between mutable and immutable
instances is completely made up. In reality, every instance of furl.furl is
mutable. The main motivation behind creating these stubs is to enlist mypy's
support in enforcing immutability when mutablility is not needed, as that lets
us more aggressively cache `furl` instances.

Furthermore, these stubs only represent current usage of `furl` in Azul. If you
want to use an aspect of `furl` that's not yet stubbed, feel free to add it.
Note however, that we intentionally do not stub many of the mostly superfluous
convenience features offered by `furl` such as converting non-string values to
strings on the fly, and the implicit handling of lists of values for the
mappings that back the query and fragment parts of a URL. These conveniences
would complicate the signatures, making them harder to read and maintain,
outweighing the value they might add.
"""

import abc
from typing import (
    Iterable,
    Mapping,
    MutableMapping,
    Self,
    Sequence,
    Union,
)

type _Args = Union[
    str,
    Mapping[str, str],
    Mapping[str, Sequence[str]],
    Sequence[tuple[str, str]]
]
type _Path = Union[
    str,
    Path,
    Sequence[str]
]
type _Fragment = _Args


class MultiMapping[K, V](
    Mapping[K, V],
    metaclass=abc.ABCMeta
):

    def getlist(self, k: K) -> Sequence[V]: ...

    def allitems(self) -> Iterable[tuple[K, V]]: ...


class MutableMultiMapping[K, V](
    MultiMapping[K, V],
    MutableMapping[K, V],
    metaclass=abc.ABCMeta
):

    def setlist(self, k: K, vs: Iterable[V]) -> Self: ...

    def addlist(self, k: K, vs: Iterable[V]) -> Self: ...


class Query:
    params: MultiMapping[str, str]

    def __str__(self) -> str: ...


class MutableQuery(Query):
    params: MutableMultiMapping[str, str]

    def add(self, args: _Args): ...


class Path:
    segments: Sequence[str]

    def __str__(self) -> str: ...


class MutablePath(Path):
    segments: list[str]

    def add(self, path: _Path): ...


class Fragment:

    def __str__(self) -> str: ...


class MutableFragment(Fragment):
    pass


class furl:
    scheme: str | None
    netloc: str | None
    host: str | None
    port: int | None
    path: Path
    args: MultiMapping[str, str]
    query: Query
    fragment: Fragment

    def __init__(self,
                 url: furl | str | None = None,
                 *,
                 scheme: str | None = None,
                 netloc: str | None = None,
                 host: str | None = None,
                 port: int | None = None,
                 path: _Path | None = None,
                 args: _Args | None = None,
                 query: _Args | None = None,
                 fragment: _Fragment | None = None,
                 ): ...

    def __str__(self) -> str: ...

    def copy(self) -> _mutable_furl: ...

    def __truediv__(self, path: _Path) -> _mutable_furl: ...


class _mutable_furl(furl):
    path: MutablePath
    args: MutableMultiMapping[str, str]
    query: MutableQuery
    fragment: MutableFragment

    def add(self,
            *,
            path: _Path | None = None,
            args: _Args | None = None,
            query: _Args | None = None,
            fragment: _Fragment | None = None,
            ) -> Self: ...

    def set(self,
            *,
            scheme: str | None = None,
            netloc: str | None = None,
            host: str | None = None,
            port: int | None = None,
            path: _Path | None = None,
            args: _Args | None = None,
            query: _Args | None = None,
            fragment: _Fragment | None = None,
            ) -> Self: ...

    def join(self, *urls: str | furl) -> Self: ...
