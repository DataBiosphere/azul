from abc import (
    ABC,
    abstractmethod,
)
from collections.abc import (
    Iterator,
)
from itertools import (
    product,
)
import logging
import math
from threading import (
    RLock,
)
from typing import (
    Any,
    ClassVar,
    Generic,
    Optional,
    Protocol,
    TYPE_CHECKING,
    Type,
    TypeVar,
    get_args,
)

import attr
from more_itertools import (
    one,
)

from azul import (
    reject,
)
from azul.types import (
    JSON,
    MutableJSON,
    MutableJSONs,
)
from azul.uuids import (
    UUIDPartition,
    validate_uuid_prefix,
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

log = logging.getLogger(__name__)

BundleUUID = str
BundleVersion = str


@attr.s(auto_attribs=True, frozen=True, kw_only=True, order=True)
class BundleFQID(SupportsLessThan):
    uuid: BundleUUID
    version: BundleVersion


@attr.s(frozen=True, auto_attribs=True, kw_only=True)
class Prefix:
    common: str = ''
    partition: int
    of_everything: ClassVar['Prefix']

    def __attrs_post_init__(self):
        validate_uuid_prefix(self.common)
        assert ':' not in self.common, self.common
        assert isinstance(self.partition, int), self.partition
        # Version 4 UUIDs specify fixed bits in the third dash-seperated
        # group. To ensure that any concatenation of common and
        # partition_prefix is a valid UUID prefix, we restrict the number of
        # characters from the concatenation to be within the first
        # dash-seperated group.
        reject(len(self.common) + self.partition > 8,
               'Invalid common prefix and partition length', self)

    @classmethod
    def parse(cls, prefix: str) -> 'Prefix':
        """
        >>> Prefix.parse('aa/1')
        Prefix(common='aa', partition=1)

        >>> p = Prefix.parse('a')
        Traceback (most recent call last):
        ...
        ValueError: ('Missing partition prefix length', 'a')

        >>> Prefix.parse('aa/')
        Traceback (most recent call last):
        ...
        azul.RequirementError: ('Prefix source cannot end in a delimiter', 'aa/', '/')

        >>> Prefix.parse('8f538f53/1').partition_prefixes() # doctest: +NORMALIZE_WHITESPACE
        Traceback (most recent call last):
        ...
        azul.RequirementError: ('Invalid common prefix and partition length',
                                Prefix(common='8f538f53', partition=1))

        >>> list(Prefix.parse('8f538f53/0').partition_prefixes())
        ['']

        >>> Prefix.parse('aa/bb')
        Traceback (most recent call last):
        ...
        ValueError: ('Partition prefix length must be an integer', 'bb')

        >>> Prefix.parse('')
        Traceback (most recent call last):
        ...
        azul.RequirementError: Cannot parse an empty prefix source
        """
        source_delimiter = '/'
        reject(prefix == '', 'Cannot parse an empty prefix source')
        reject(prefix.endswith(source_delimiter),
               'Prefix source cannot end in a delimiter', prefix, source_delimiter)
        try:
            entry, partition = prefix.split(source_delimiter)
        except ValueError:
            raise ValueError('Missing partition prefix length', prefix)
        try:
            partition = int(partition)
        except ValueError:
            raise ValueError('Partition prefix length must be an integer', partition)
        validate_uuid_prefix(entry)
        return cls(common=entry, partition=partition)

    def partition_prefixes(self) -> Iterator[str]:
        """
        >>> list(Prefix.parse('/0').partition_prefixes())
        ['']

        >>> list(Prefix.parse('/1').partition_prefixes())
        ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f']

        >>> len(list(Prefix.parse('/2').partition_prefixes()))
        256
        """
        partition_prefixes = map(''.join, product('0123456789abcdef',
                                                  repeat=self.partition))
        for partition_prefix in partition_prefixes:
            validate_uuid_prefix(self.common + partition_prefix)
            yield partition_prefix

    def __str__(self):
        """
        >>> s = 'aa/1'
        >>> s == str(Prefix.parse(s))
        True
        """
        return f'{self.common}/{self.partition}'


Prefix.of_everything = Prefix.parse('/0')

SOURCE_SPEC = TypeVar('SOURCE_SPEC', bound='SourceSpec')


@attr.s(frozen=True, auto_attribs=True, kw_only=True)
class SourceSpec(ABC, Generic[SOURCE_SPEC]):
    """
    The name of a repository source containing bundles to index. A repository
    has at least one source. Repository plugins whose repository source names
    are structured might want to implement this abstract class. Plugins that
    have simple unstructured names may want to use :class:`SimpleSourceSpec`.
    """

    prefix: Prefix

    @classmethod
    @abstractmethod
    def parse(cls, spec: str) -> SOURCE_SPEC:
        raise NotImplementedError

    @classmethod
    def _parse(cls, spec: str) -> tuple[str, Prefix]:
        rest, sep, prefix = spec.rpartition(':')
        reject(sep == '', 'Invalid source specification', spec)
        prefix = Prefix.parse(prefix)
        return rest, prefix

    @abstractmethod
    def __str__(self) -> str:
        raise NotImplementedError

    def contains(self, other: 'SourceSpec') -> bool:
        """
        >>> p = SimpleSourceSpec.parse

        >>> p('foo:4/0').contains(p('foo:42/0'))
        True

        >>> p('foo:42/0').contains(p('foo:4/0'))
        False

        >>> p('foo:42/0').contains(p('foo:42/0'))
        True

        >>> p('foo:42/0').contains(p('foo:42/1'))
        True

        >>> p('foo:1/0').contains(p('foo:2/0'))
        False
        """
        assert isinstance(other, SourceSpec), (self, other)
        return other.prefix.common.startswith(self.prefix.common)


@attr.s(frozen=True, auto_attribs=True, kw_only=True)
class SimpleSourceSpec(SourceSpec['SimpleSourceSpec']):
    """
    Default implementation for unstructured source names.
    """
    name: str

    @classmethod
    def parse(cls, spec: str) -> 'SimpleSourceSpec':
        """
        >>> SimpleSourceSpec.parse('https://foo.edu:12/0') # doctest: +NORMALIZE_WHITESPACE
        SimpleSourceSpec(prefix=Prefix(common='12',
                                       partition=0),
                         name='https://foo.edu')

        >>> SimpleSourceSpec.parse('foo')
        Traceback (most recent call last):
        ...
        azul.RequirementError: ('Invalid source specification', 'foo')

        >>> SimpleSourceSpec.parse('foo:8F53/0')
        Traceback (most recent call last):
        ...
        azul.uuids.InvalidUUIDPrefixError: '8F53' is not a valid UUID prefix.

        >>> SimpleSourceSpec.parse('https:foo.edu/0')
        Traceback (most recent call last):
        ...
        azul.uuids.InvalidUUIDPrefixError: 'foo.edu' is not a valid UUID prefix.
        """
        name, prefix = cls._parse(spec)
        self = cls(prefix=prefix, name=name)
        assert spec == str(self), spec
        return self

    def __str__(self) -> str:
        """
        >>> s = 'foo:bar/baz:/0'
        >>> s == str(SimpleSourceSpec.parse(s))
        True

        >>> s = 'foo:bar/baz:12/0'
        >>> s == str(SimpleSourceSpec.parse(s))
        True

        >>> s = 'foo:bar/baz:12/2'
        >>> s == str(SimpleSourceSpec.parse(s))
        True
        """
        return f'{self.name}:{self.prefix}'

    def contains(self, other: 'SourceSpec') -> bool:
        """
        >>> p = SimpleSourceSpec.parse

        >>> p('foo:/0').contains(p('foo:/0'))
        True

        >>> p('foo:/0').contains(p('bar:/0'))
        False
        """
        return (
            isinstance(other, SimpleSourceSpec)
            and super().contains(other)
            and self.name == other.name
        )


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

    _lookup: ClassVar[dict[tuple[Type['SourceRef'], str], 'SourceRef']] = {}
    _lookup_lock = RLock()

    def __new__(cls: Type[SOURCE_REF], *, id: str, spec: SOURCE_SPEC) -> SOURCE_REF:
        """
        Interns instances by their ID and ensures that names are unambiguous
        for any given ID. Two different sources may still use the same name.

        >>> class S(SourceRef): pass
        >>> a, b  = SimpleSourceSpec.parse('a:/0'), SimpleSourceSpec.parse('b:/0')

        >>> S(id='1', spec=a) is S(id='1', spec=a)
        True

        >>> S(id='1', spec=a) is S(id='2', spec=a)
        False

        FIXME: Disallow two refs with same ID and different names
               https://github.com/DataBiosphere/azul/issues/3250

        >>> S(id='1', spec=b) # doctest: +NORMALIZE_WHITESPACE
        S(id='1', spec=SimpleSourceSpec(prefix=Prefix(common='',
                                                      partition=0),
                                        name='b'))

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
                self = lookup[cls, id, spec]
            except KeyError:
                self = super().__new__(cls)
                # noinspection PyArgumentList
                self.__init__(id=id, spec=spec)
                lookup[cls, id, spec] = self
            else:
                assert self.id == id
                assert self.spec == spec, (self.spec, spec)
            return self

    def to_json(self):
        return dict(id=self.id, spec=str(self.spec))

    @classmethod
    def from_json(cls, ref: JSON) -> 'SourceRef':
        return cls(id=ref['id'], spec=cls.spec_cls().parse(ref['spec']))

    @classmethod
    def spec_cls(cls) -> Type[SourceSpec]:
        base_cls = one(getattr(cls, '__orig_bases__'))
        spec_cls, ref_cls = get_args(base_cls)
        return spec_cls


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


class BundlePartition(UUIDPartition['BundlePartition']):
    """
    A binary partitioning of the UUIDs of outer entities in a bundle.
    """

    #: 512 caused timeouts writing contributions, even in the retry Lambda
    max_partition_size: ClassVar[int] = 256

    def divisions(self, num_entities: int) -> int:
        return math.ceil(num_entities / self.max_partition_size)

    def __attrs_post_init__(self):
        super().__attrs_post_init__()
        # Most bits in a v4 or v5 UUID are pseudo-random, including the leading
        # 32 bits but those are followed by a couple of deterministic ones.
        # For simplicity, we'll limit ourselves to 2 ** 32 leaf partitions.
        reject(self.prefix_length > 32)
