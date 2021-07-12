from abc import (
    ABC,
    abstractmethod,
)
from itertools import (
    product,
)
import logging
from threading import (
    RLock,
)
from typing import (
    Any,
    ClassVar,
    Dict,
    Generic,
    Iterator,
    Optional,
    Protocol,
    TYPE_CHECKING,
    Tuple,
    Type,
    TypeVar,
    get_args,
)

import attr
from more_itertools import (
    first,
    one,
)

from azul import (
    cached_property,
    config,
    reject,
)
from azul.types import (
    JSON,
    MutableJSON,
    MutableJSONs,
)
from azul.uuids import (
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
    partition: Optional[int]
    of_everything: ClassVar['Prefix']

    def __attrs_post_init__(self):
        validate_uuid_prefix(self.common)
        assert ':' not in self.common, self.common
        if self.partition:
            assert isinstance(self.partition, int), self.partition
            partition = self.partition
        else:
            partition = config.partition_prefix_length
        partition_prefix = first(self._partition_generator(partition))
        validate_uuid_prefix(self.common + partition_prefix)

    @classmethod
    def parse(cls, prefix: str) -> 'Prefix':
        """
        >>> Prefix.parse('aa/1')
        Prefix(common='aa', partition=1)

        >>> p = Prefix.parse('aa')
        >>> p
        Prefix(common='aa', partition=None)
        >>> from unittest.mock import patch
        >>> import os
        >>> with patch.dict(os.environ, AZUL_PARTITION_PREFIX_LENGTH='2'):
        ...     p.effective
        Prefix(common='aa', partition=2)

        >>> Prefix.parse('aa/')
        Traceback (most recent call last):
        ...
        azul.RequirementError: ('Prefix source cannot end in a delimiter.', 'aa/', '/')

        >>> Prefix.parse('8f538f53/1')
        Traceback (most recent call last):
        ...
        azul.uuids.InvalidUUIDPrefixError: '8f538f530' is not a valid UUID prefix.
        """
        source_delimiter = '/'
        reject(prefix.endswith(source_delimiter),
               'Prefix source cannot end in a delimiter.', prefix, source_delimiter)
        if prefix == '':
            entry = ''
            partition = None
        else:
            try:
                entry, partition = prefix.split(source_delimiter)
            except ValueError:
                entry = prefix
                partition = None
            if partition:
                try:
                    partition = int(partition)
                except ValueError:
                    raise ValueError('Partition prefix length must be an integer.', partition)
        validate_uuid_prefix(entry)
        return cls(common=entry, partition=partition)

    @cached_property
    def effective(self) -> 'Prefix':
        if self.partition is None:
            return attr.evolve(self, partition=config.partition_prefix_length)
        else:
            return self

    def partition_prefixes(self) -> Iterator[str]:
        """
        >>> list(Prefix.parse('/0').partition_prefixes())
        ['']

        >>> list(Prefix.parse('/1').partition_prefixes())
        ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f']

        >>> len(set(Prefix.parse('/2').partition_prefixes()))
        256
        """
        for partition_prefix in self._partition_generator(self.partition):
            validate_uuid_prefix(self.common + partition_prefix)
            yield partition_prefix

    def _partition_generator(self, partition) -> Iterator[str]:
        return map(''.join, product('0123456789abcdef', repeat=partition))

    def __str__(self):
        """
        >>> s = 'aa'
        >>> s == str(Prefix.parse(s))
        True

        >>> s = 'aa/1'
        >>> s == str(Prefix.parse(s))
        True
        """
        if self.partition is None:
            return self.common
        else:
            return f'{self.common}/{self.partition}'


Prefix.of_everything = Prefix.parse('').effective

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
    def parse(cls, spec: str) -> SOURCE_SPEC:
        _spec, sep, prefix = spec.rpartition(':')
        reject(sep == '',
               'Source specifications must end in a colon followed by an optional UUID prefix')
        prefix = Prefix.parse(prefix)
        if _spec.startswith('tdr'):
            # Import locally to avoid cyclical import
            from azul.terra import (
                TDRSourceSpec,
            )
            self = TDRSourceSpec._parse(_spec, prefix)
        else:
            self = SimpleSourceSpec._parse(_spec, prefix)
        assert spec == str(self), (spec, str(self), self)
        return self

    @abstractmethod
    def __str__(self) -> str:
        raise NotImplementedError

    @cached_property
    def effective(self) -> SOURCE_SPEC:
        return attr.evolve(self, prefix=self.prefix.effective)


@attr.s(frozen=True, auto_attribs=True, kw_only=True)
class SimpleSourceSpec(SourceSpec['SimpleSourceSpec']):
    """
    Default implementation for unstructured source names.
    """
    name: str

    @classmethod
    def _parse(cls, spec: str, prefix: Prefix) -> 'SimpleSourceSpec':
        """
        >>> SimpleSourceSpec.parse('https://foo.edu:12') # doctest: +NORMALIZE_WHITESPACE
        SimpleSourceSpec(prefix=Prefix(common='12', partition=None),
                         name='https://foo.edu')

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
        return cls(prefix=prefix, name=spec)

    def __str__(self) -> str:
        """
        >>> s = 'foo:bar/baz:'
        >>> s == str(SimpleSourceSpec.parse(s))
        True

        >>> s = 'foo:bar/baz:12'
        >>> s == str(SimpleSourceSpec.parse(s))
        True

        >>> s = 'foo:bar/baz:12/2'
        >>> s == str(SimpleSourceSpec.parse(s))
        True
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

        FIXME: Disallow two refs with same id and different names
               https://github.com/DataBiosphere/azul/issues/3250

        >>> S(id='1', spec=b) # doctest: +NORMALIZE_WHITESPACE
        S(id='1', spec=SimpleSourceSpec(prefix=Prefix(common='', partition=None),
                                        name='b'))

        Two specs with same name and id but different prefix string

        >>> a1 = SimpleSourceSpec.parse('a:42')
        >>> S(id='1', spec=a1) # doctest: +NORMALIZE_WHITESPACE
        S(id='1', spec=SimpleSourceSpec(prefix=Prefix(common='42', partition=None),
                                        name='a'))

        Two specs with same name and id but different partition prefix

        >>> a2 = SimpleSourceSpec.parse('a:/2')
        >>> S(id='1', spec=a2) # doctest: +NORMALIZE_WHITESPACE
        S(id='1', spec=SimpleSourceSpec(prefix=Prefix(common='', partition=2),
                                        name='a'))

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
        return cls(spec=cls.spec_cls().parse(ref['spec']).effective, id=ref['id'])

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
