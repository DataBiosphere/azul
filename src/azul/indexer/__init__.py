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
    reject,
    require,
)
from azul.types import (
    JSON,
    MutableJSON,
    MutableJSONs,
)

# FIXME: Remove hacky import of SupportsLessThan
#        https://github.com/DataBiosphere/azul/issues/2783
from azul.uuids import (
    validate_uuid_prefix,
)

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


SOURCE_SPEC = TypeVar('SOURCE_SPEC', bound='SourceSpec')


@attr.s(frozen=True, auto_attribs=True, kw_only=True)
class SourceSpec(ABC, Generic[SOURCE_SPEC]):
    """
    The name of a repository source containing bundles to index. A repository
    has at least one source. Repository plugins whose repository source names
    are structured might want to implement this abstract class. Plugins that
    have simple unstructured names may want to use :class:`SimpleSourceSpec`.
    """

    prefix: str = ''

    def __attrs_post_init__(self):
        validate_uuid_prefix(self.prefix)
        assert ':' not in self.prefix, self.prefix

    @classmethod
    @abstractmethod
    def parse(cls, spec: str) -> SOURCE_SPEC:
        raise NotImplementedError

    @abstractmethod
    def __str__(self) -> str:
        raise NotImplementedError


@attr.s(frozen=True, auto_attribs=True, kw_only=True)
class SimpleSourceSpec(SourceSpec['SimpleSourceSpec']):
    """
    Default implementation for unstructured source names.
    """
    name: str

    @classmethod
    def parse(cls, spec: str) -> 'SimpleSourceSpec':
        """
        >>> SimpleSourceSpec.parse('https://foo.edu:12')
        SimpleSourceSpec(prefix='12', name='https://foo.edu')

        >>> SimpleSourceSpec.parse('foo')
        Traceback (most recent call last):
        ...
        azul.RequirementError: Source specifications must end in a colon followed by an optional UUID prefix

        >>> SimpleSourceSpec.parse('foo:8F53')
        Traceback (most recent call last):
        ...
        azul.uuids.InvalidUUIDPrefixError: '8F53' is not a valid UUID prefix.

        >>> SimpleSourceSpec.parse('https://foo.edu')
        Traceback (most recent call last):
        ...
        azul.uuids.InvalidUUIDPrefixError: '//foo.edu' is not a valid UUID prefix.
        """

        # FIXME: Move parsing of prefix to SourceSpec
        #        https://github.com/DataBiosphere/azul/issues/3073
        name, sep, prefix = spec.rpartition(':')
        reject(sep == '',
               'Source specifications must end in a colon followed by an optional UUID prefix')
        return cls(prefix=prefix, name=name)

    def __str__(self) -> str:
        """
        >>> str(SimpleSourceSpec(prefix='12', name='foo:bar/baz'))
        'foo:bar/baz:12'
        """
        return f'{self.name}:{self.prefix}'


SOURCE_REF = TypeVar('SOURCE_REF', bound='SourceRef')


@attr.s(auto_attribs=True, frozen=True, kw_only=True)
class SourceRef(Generic[SOURCE_SPEC, SOURCE_REF]):
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
    spec: SOURCE_SPEC

    _lookup: ClassVar[Dict[Tuple[Type['SourceRef'], str], 'SourceRef']] = {}
    _lookup_lock = RLock()

    def __new__(cls: Type[SOURCE_REF], *, id: str, spec: SOURCE_SPEC) -> SOURCE_REF:
        """
        Interns instances by their ID and ensures that names are unambiguous
        for any given ID. Two different sources may still use the same name.

        >>> class S(SourceRef): pass
        >>> a, b  = SimpleSourceSpec.parse('a:'), SimpleSourceSpec.parse('b:')

        >>> S(id='1', spec=a) is S(id='1', spec=a)
        True

        >>> S(id='1', spec=a) is S(id='2', spec=a)
        False

        >>> S(id='1', spec=b) # doctest: +NORMALIZE_WHITESPACE
        Traceback (most recent call last):
        ...
        azul.RequirementError: ('Ambiguous source specs for same ID.',
                                SimpleSourceSpec(prefix='', name='a'),
                                SimpleSourceSpec(prefix='', name='b'),
                                '1')

        Interning is done per class:

        >>> class T(S): pass
        >>> T(id='1', spec=a) is S(id='1', spec=a)
        False

        >>> T(id='1', spec=a) == S(id='1', spec=a)
        False
        """
        with cls._lookup_lock:
            lookup = cls._lookup
            try:
                self = lookup[cls, id]
            except KeyError:
                self = super().__new__(cls)
                # noinspection PyArgumentList
                self.__init__(id=id, spec=spec)
                lookup[cls, id] = self
            else:
                assert self.id == id
                require(self.spec == spec,
                        'Ambiguous source specs for same ID.', self.spec, spec, id)
            return self

    def to_json(self):
        return dict(id=self.id, spec=str(self.spec))


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
