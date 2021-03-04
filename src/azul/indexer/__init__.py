from abc import (
    ABC,
    abstractmethod,
)
from threading import (
    RLock,
)
from typing import (
    Any,
    ClassVar,
    Dict,
    Generic,
    Optional,
    Protocol,
    TYPE_CHECKING,
    Tuple,
    Type,
    TypeVar,
)

import attr

from azul import (
    require,
)
from azul.types import (
    JSON,
    MutableJSON,
    MutableJSONs,
)

# FIXME: Remove hacky import of SupportsLessThan
#        https://github.com/DataBiosphere/azul/issues/2783
if TYPE_CHECKING:
    from _typeshed import (
        SupportsLessThan,
    )
else:
    class SupportsLessThan(Protocol):

        def __lt__(self, __other: Any) -> bool: ...

BundleUUID = str
BundleVersion = str


@attr.s(auto_attribs=True, frozen=True, kw_only=True, order=True)
class BundleFQID(SupportsLessThan):
    uuid: BundleUUID
    version: BundleVersion


SOURCE_NAME = TypeVar('SOURCE_NAME', bound='SourceName')


# FIXME: Rename to SourceSpec/SOURCE_SPEC, and all .name to .spec
#        https://github.com/DataBiosphere/azul/issues/2843
class SourceName(ABC, Generic[SOURCE_NAME]):
    """
    The name of a repository source containing bundles to index. A repository
    has at least one source. Repository plugins whose repository source names
    are structured might want to implement this abstract class. Plugins that
    have simple unstructured names may want to use :class:`StringSourceName`.
    """

    @classmethod
    @abstractmethod
    def parse(cls, name: str) -> SOURCE_NAME:
        raise NotImplementedError

    @abstractmethod
    def __str__(self) -> str:
        raise NotImplementedError


class SimpleSourceName(str, SourceName['SimpleSourceName']):
    """
    Default implementation for unstructured source names.
    """

    @classmethod
    def parse(cls, name: str) -> 'SimpleSourceName':
        return cls(name)


SOURCE_REF = TypeVar('SOURCE_REF', bound='SourceRef')


@attr.s(auto_attribs=True, frozen=True, kw_only=True)
class SourceRef(Generic[SOURCE_NAME, SOURCE_REF]):
    """
    A reference to a repository source containing bundles to index. A repository
    has at least one source. A source is primarily referenced by its ID but we
    drag the name along to 1) avoid repeatedly looking it up and 2) ensure that
    the mapping between the two doesn't change while we index a source.

    Instances of this class are interned: within a Python interpreter process,
    there will only ever be one instance of this class for any given ID. There
    may be an instance of a subclass of this class that has the same ID as an
    instance of this class or another subclass of this class.

    Note to plugin implementers: Since the source ID can't be assumed to be
    globally unique, plugins should subclass this class, even if the subclass
    body is empty.
    """
    id: str
    name: SOURCE_NAME

    _lookup: ClassVar[Dict[Tuple[Type['SourceRef'], str], 'SourceRef']] = {}
    _lookup_lock = RLock()

    def __new__(cls: Type[SOURCE_REF], *, id: str, name: SOURCE_NAME) -> SOURCE_REF:
        """
        Interns instances by their ID and ensures that names are unambiguous
        for any given ID. Two different sources may still use the same name.

        >>> class S(SourceRef): pass
        >>> a, b  = SimpleSourceName.parse('a'), SimpleSourceName.parse('b')

        >>> S(id='1', name=a) is S(id='1', name=a)
        True

        >>> S(id='1', name=a) is S(id='2', name=a)
        False

        >>> S(id='1', name=b)
        Traceback (most recent call last):
        ...
        azul.RequirementError: ('Ambiguous source names for same ID.', 'a', 'b', '1')

        Interning is done per class:

        >>> class T(S): pass
        >>> T(id='1', name=a) is S(id='1', name=a)
        False

        >>> T(id='1', name=a) == S(id='1', name=a)
        False
        """
        with cls._lookup_lock:
            lookup = cls._lookup
            try:
                self = lookup[cls, id]
            except KeyError:
                self = super().__new__(cls)
                # noinspection PyArgumentList
                self.__init__(id=id, name=name)
                lookup[cls, id] = self
            else:
                assert self.id == id
                require(self.name == name,
                        'Ambiguous source names for same ID.', self.name, name, id)
            return self

    def to_json(self):
        return dict(id=self.id, name=str(self.name))


@attr.s(auto_attribs=True, frozen=True, kw_only=True, order=True)
class SourcedBundleFQID(BundleFQID, Generic[SOURCE_REF]):
    source: SOURCE_REF

    def upcast(self):
        return BundleFQID(uuid=self.uuid,
                          version=self.version)


@attr.s(auto_attribs=True, kw_only=True)
class Bundle(ABC, Generic[SOURCE_REF]):
    fqid: SourcedBundleFQID[SOURCE_REF]
    manifest: MutableJSONs
    """
    Each item of the `manifest` attribute's value has this shape:
    {
        'content-type': 'application/json; dcp-type="metadata/biomaterial"',
        'crc32c': 'fd239631',
        'indexed': True,
        'name': 'cell_suspension_0.json',
        's3_etag': 'aa31c093cc816edb1f3a42e577872ec6',
        'sha1': 'f413a9a7923dee616309e4f40752859195798a5d',
        'sha256': 'ea4c9ed9e53a3aa2ca4b7dffcacb6bbe9108a460e8e15d2b3d5e8e5261fb043e',
        'size': 1366,
        'uuid': '0136ebb4-1317-42a0-8826-502fae25c29f',
        'version': '2019-05-16T162155.020000Z'
    }
    """
    metadata_files: MutableJSON

    @property
    def uuid(self) -> BundleUUID:
        return self.fqid.uuid

    @property
    def version(self) -> BundleVersion:
        return self.fqid.version

    @abstractmethod
    def drs_path(self, manifest_entry: JSON) -> Optional[str]:
        """
        Return the path component of a DRS URI to a data file in this bundle,
        or None if the data file is not accessible via DRS.

        :param manifest_entry: the manifest entry of the data file.
        """
        raise NotImplementedError
