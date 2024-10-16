from abc import (
    ABCMeta,
    abstractmethod,
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
    ClassVar,
    Generic,
    Iterator,
    Self,
    TypeVar,
    TypedDict,
)

import attrs

from azul import (
    CatalogName,
    RequirementError,
    config,
    reject,
)
from azul.types import (
    AnyJSON,
    JSON,
    MutableJSON,
    SupportsLessAndGreaterThan,
    get_generic_type_params,
)
from azul.uuids import (
    UUIDPartition,
    validate_uuid_prefix,
)

log = logging.getLogger(__name__)

BundleUUID = str
BundleVersion = str


@attrs.frozen(kw_only=True, order=True)
class BundleFQID(SupportsLessAndGreaterThan):
    """
    >>> list(sorted([
    ...     BundleFQID(uuid='d', version='e'),
    ...     BundleFQID(uuid='a', version='c'),
    ...     BundleFQID(uuid='a', version='b'),
    ... ]))
    ... # doctest: +NORMALIZE_WHITESPACE
    [BundleFQID(uuid='a', version='b'),
    BundleFQID(uuid='a', version='c'),
    BundleFQID(uuid='d', version='e')]
    """
    uuid: BundleUUID = attrs.field(order=str.lower)
    version: BundleVersion = attrs.field(order=str.lower)

    def to_json(self) -> MutableJSON:
        return attrs.asdict(self, recurse=False)


class BundleFQIDJSON(TypedDict):
    uuid: BundleUUID
    version: BundleVersion


@attrs.frozen(kw_only=True)
class Prefix:
    common: str = ''
    partition: int
    of_everything: ClassVar['Prefix']

    digits = '0123456789abcdef'

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
    def parse(cls, prefix: str) -> Self:
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
        ['8f538f53']

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

    @classmethod
    def for_main_deployment(cls, num_subgraphs: int) -> Self:
        """
        A prefix that is expected to rarely exceed 8192 subgraphs per partition

        >>> str(Prefix.for_main_deployment(0))
        Traceback (most recent call last):
        ...
        ValueError: math domain error

        >>> str(Prefix.for_main_deployment(1))
        '/0'

        >>> cases = [-1, 0, 1, 2]

        >>> n = 8192
        >>> [str(Prefix.for_main_deployment(n + i)) for i in cases]
        ['/0', '/0', '/1', '/1']

        Sources with this many bundles are very rare, so we have a generous
        margin of error surrounding this cutoff point

        >>> n = 8192 * 16
        >>> [str(Prefix.for_main_deployment(n + i)) for i in cases]
        ['/1', '/1', '/2', '/2']
        """
        partition = cls._prefix_length(num_subgraphs, 8192)
        return cls(common='', partition=partition)

    @classmethod
    def for_lesser_deployment(cls, num_subgraphs: int) -> Self:
        """
        A prefix that yields an average of approximately 24 subgraphs per
        source, using an experimentally derived heuristic formula designed to
        minimize manual adjustment of the computed common prefixes. The
        partition prefix length is always 1, even though some partitions may be
        empty, to provide test coverage for handling multiple partitions.

        >>> str(Prefix.for_lesser_deployment(0))
        Traceback (most recent call last):
        ...
        ValueError: math domain error

        >>> str(Prefix.for_lesser_deployment(1))
        '/1'

        >>> cases = [-1, 0, 1, 2]

        >>> n = 64
        >>> [str(Prefix.for_lesser_deployment(n + i)) for i in cases]
        ['/1', '/1', '0/1', '1/1']

        >>> n = 64 * 16
        >>> [str(Prefix.for_lesser_deployment(n + i)) for i in cases]
        ['e/1', 'f/1', '00/1', '10/1']
        """
        digits = f'{num_subgraphs - 1:x}'[::-1]
        length = cls._prefix_length(num_subgraphs, 64)
        assert length < len(digits), num_subgraphs
        return cls(common=digits[:length], partition=1)

    @classmethod
    def _prefix_length(cls, n, m) -> int:
        return max(0, math.ceil(math.log(n / m, len(cls.digits))))

    def partition_prefixes(self) -> Iterator[str]:
        """
        >>> list(Prefix.parse('/0').partition_prefixes())
        ['']

        >>> list(Prefix.parse('a/1').partition_prefixes())
        ['a0', 'a1', 'a2', 'a3', 'a4', 'a5', 'a6', 'a7', 'a8', 'a9', 'aa', 'ab', 'ac', 'ad', 'ae', 'af']

        >>> len(list(Prefix.parse('/2').partition_prefixes()))
        256
        """
        for partition_prefix_digits in product(self.digits, repeat=self.partition):
            complete_prefix = ''.join((self.common, *partition_prefix_digits))
            validate_uuid_prefix(complete_prefix)
            yield complete_prefix

    @property
    def num_partitions(self) -> int:
        """
        Equivalent to `len(self.partition_prefixes())`, but more efficient.

        >>> Prefix.parse('aa/0').num_partitions
        1
        >>> Prefix.parse('/3').num_partitions
        4096
        >>> Prefix.parse('aa/3').num_partitions
        4096
        """
        return len(self.digits) ** self.partition

    def __str__(self):
        """
        >>> s = 'aa/1'
        >>> s == str(Prefix.parse(s))
        True
        """
        return f'{self.common}/{self.partition}'

    def __len__(self):
        """
        >>> len(Prefix.parse('aa/0'))
        2
        >>> len(Prefix.parse('/3'))
        3
        >>> len(Prefix.parse('aa/3'))
        5
        """
        return len(self.common) + self.partition


Prefix.of_everything = Prefix.parse('/0')

SOURCE_SPEC = TypeVar('SOURCE_SPEC', bound='SourceSpec')


@attrs.frozen(kw_only=True)
class SourceSpec(Generic[SOURCE_SPEC], metaclass=ABCMeta):
    """
    The name of a repository source containing bundles to index. A repository
    has at least one source. Repository plugins whose repository source names
    are structured might want to implement this abstract class. Plugins that
    have simple unstructured names may want to use :class:`SimpleSourceSpec`.
    """

    prefix: Prefix | None

    @classmethod
    @abstractmethod
    def parse(cls, spec: str) -> SOURCE_SPEC:
        raise NotImplementedError

    @classmethod
    def _parse(cls, spec: str) -> tuple[str, Prefix | None]:
        rest, sep, prefix = spec.rpartition(':')
        reject(sep == '', 'Invalid source specification', spec)
        prefix = Prefix.parse(prefix) if prefix else None
        return rest, prefix

    @property
    def _prefix_str(self) -> str:
        return '' if self.prefix is None else str(self.prefix)

    @abstractmethod
    def __str__(self) -> str:
        raise NotImplementedError

    def eq_ignoring_prefix(self, other: Self) -> bool:
        """
        >>> p = SimpleSourceSpec.parse

        >>> p('foo:4/0').eq_ignoring_prefix(p('foo:42/0'))
        True

        >>> p('foo:4/0').eq_ignoring_prefix(p('bar:4/0'))
        False
        """
        return self == attrs.evolve(other, prefix=self.prefix)


@attrs.frozen(kw_only=True)
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
        return f'{self.name}:{self._prefix_str}'


class SourceJSON(TypedDict):
    id: str
    spec: str


SOURCE_REF = TypeVar('SOURCE_REF', bound='SourceRef')


@attrs.frozen(kw_only=True, order=True)
class SourceRef(SupportsLessAndGreaterThan, Generic[SOURCE_SPEC, SOURCE_REF]):
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

    >>> spec = SimpleSourceSpec(name='', prefix=(Prefix(partition=0)))
    >>> list(sorted([
    ...     SourceRef(id='d', spec=spec),
    ...     SourceRef(id='a', spec=spec),
    ... ]))
    ... # doctest: +NORMALIZE_WHITESPACE
    [SourceRef(id='a', spec=SimpleSourceSpec(prefix=Prefix(common='', partition=0), name='')),
    SourceRef(id='d', spec=SimpleSourceSpec(prefix=Prefix(common='', partition=0), name=''))]

    """
    id: str = attrs.field(order=str.lower)
    spec: SOURCE_SPEC = attrs.field(order=False)

    _lookup: ClassVar[dict[tuple[type['SourceRef'], str, 'SourceSpec'], 'SourceRef']] = {}
    _lookup_lock = RLock()

    def __new__(cls: type[SOURCE_REF], *, id: str, spec: SOURCE_SPEC) -> SOURCE_REF:
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

    def to_json(self) -> SourceJSON:
        return dict(id=self.id, spec=str(self.spec))

    @classmethod
    def from_json(cls, ref: SourceJSON) -> 'SourceRef':
        return cls(id=ref['id'], spec=cls.spec_cls().parse(ref['spec']))

    @classmethod
    def spec_cls(cls) -> type[SourceSpec]:
        spec_cls, ref_cls = get_generic_type_params(cls, SourceSpec, SourceRef)
        return spec_cls


class SourcedBundleFQIDJSON(BundleFQIDJSON):
    source: SourceJSON


BUNDLE_FQID = TypeVar('BUNDLE_FQID', bound='SourcedBundleFQID')


@attrs.frozen(kw_only=True, order=True)
class SourcedBundleFQID(BundleFQID, Generic[SOURCE_REF]):
    """
    >>> spec = SimpleSourceSpec(name='', prefix=(Prefix(partition=0)))
    >>> list(sorted([
    ...     SourcedBundleFQID(uuid='d', version='e', source=SourceRef(id='1', spec=spec)),
    ...     SourcedBundleFQID(uuid='a', version='c', source=SourceRef(id='2', spec=spec)),
    ...     SourcedBundleFQID(uuid='a', version='b', source=SourceRef(id='3', spec=spec)),
    ... ]))
    ... # doctest: +NORMALIZE_WHITESPACE
    [SourcedBundleFQID(uuid='a', version='b',
        source=SourceRef(id='3', spec=SimpleSourceSpec(prefix=Prefix(common='', partition=0), name=''))),
    SourcedBundleFQID(uuid='a', version='c',
        source=SourceRef(id='2', spec=SimpleSourceSpec(prefix=Prefix(common='', partition=0), name=''))),
    SourcedBundleFQID(uuid='d', version='e',
        source=SourceRef(id='1', spec=SimpleSourceSpec(prefix=Prefix(common='', partition=0), name='')))]
    """

    source: SOURCE_REF

    @classmethod
    def source_ref_cls(cls) -> type[SOURCE_REF]:
        ref_cls, = get_generic_type_params(cls, SourceRef)
        return ref_cls

    @classmethod
    def from_json(cls, json: SourcedBundleFQIDJSON) -> 'SourcedBundleFQID':
        json = dict(json)
        source = cls.source_ref_cls().from_json(json.pop('source'))
        return cls(source=source, **json)

    def upcast(self) -> BundleFQID:
        return BundleFQID(uuid=self.uuid,
                          version=self.version)

    def to_json(self) -> SourcedBundleFQIDJSON:
        return dict(super().to_json(),
                    source=self.source.to_json())


@attrs.define(kw_only=True)
class Bundle(Generic[BUNDLE_FQID], metaclass=ABCMeta):
    fqid: BUNDLE_FQID

    @property
    def uuid(self) -> BundleUUID:
        return self.fqid.uuid

    @property
    def version(self) -> BundleVersion:
        return self.fqid.version

    @abstractmethod
    def drs_uri(self, manifest_entry: JSON) -> str | None:
        """
        Return the DRS URI to a data file in this bundle, or None if the data
        file is not accessible via DRS.

        :param manifest_entry: the manifest entry of the data file.
        """
        raise NotImplementedError

    def _reject_joiner(self, value: AnyJSON):
        if isinstance(value, dict):
            # Since the keys in the metadata files and manifest are pre-defined,
            # we save some time here by not looking for the joiner in the keys.
            for v in value.values():
                self._reject_joiner(v)
        elif isinstance(value, list):
            for v in value:
                self._reject_joiner(v)
        elif isinstance(value, str):
            if config.manifest_column_joiner in value:
                msg = f'{config.manifest_column_joiner!r} is disallowed in metadata'
                raise RequirementError(msg, self.fqid)

    @abstractmethod
    def reject_joiner(self, catalog: CatalogName):
        """
        Raise a requirement error if the given string is found in the bundle
        """
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def canning_qualifier(cls) -> str:
        """
        Short string prepended to the file extension to distinguish between
        canned bundle formats originating from different plugins.
        """
        raise NotImplementedError

    @classmethod
    def from_json(cls, fqid: BUNDLE_FQID, json_: JSON) -> 'Bundle':
        raise NotImplementedError

    @abstractmethod
    def to_json(self) -> MutableJSON:
        raise NotImplementedError


BUNDLE = TypeVar('BUNDLE', bound=Bundle)


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
