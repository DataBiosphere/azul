from abc import (
    ABCMeta,
)
from itertools import (
    product,
)
import math
from typing import (
    ClassVar,
    Iterator,
    Self,
    cast,
)

import attrs

from azul.field_type import (
    FieldTypes,
    pass_thru_str,
)
from azul.lib import (
    R,
)
from azul.lib.attrs import (
    DiscriminatingPolymorphicSerializableAttrs,
    SerializableAttrs,
)
from azul.lib.json import (
    DynamicPolymorphicSerializable,
    Parseable,
)
from azul.lib.types import (
    SupportsLessAndGreaterThan,
    derived_type_params,
)
from azul.lib.uuids import (
    validate_uuid_prefix,
)


@attrs.frozen(kw_only=True)
class Prefix(Parseable):
    common: str = ''
    partition: int
    of_everything: ClassVar[Prefix]

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
        assert len(self.common) + self.partition <= 8, R(
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
        AssertionError: R('Prefix source cannot end in a delimiter', 'aa/', '/')

        >>> Prefix.parse('8F53/0')
        Traceback (most recent call last):
        ...
        azul.lib.uuids.InvalidUUIDPrefixError: '8F53' is not a valid UUID prefix.

        >>> Prefix.parse('https:foo.edu/0')
        Traceback (most recent call last):
        ...
        azul.lib.uuids.InvalidUUIDPrefixError: 'https:foo.edu' is not a valid UUID prefix.

        >>> Prefix.parse('8f538f53/1').partition_prefixes() # doctest: +NORMALIZE_WHITESPACE
        Traceback (most recent call last):
        ...
        AssertionError: R('Invalid common prefix and partition length',
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
        AssertionError: R('Cannot parse an empty prefix source')
        """
        source_delimiter = '/'
        assert prefix != '', R('Cannot parse an empty prefix source')
        assert not prefix.endswith(source_delimiter), R(
            'Prefix source cannot end in a delimiter', prefix, source_delimiter)
        partition: str | int
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
    def for_main_deployment(cls, num_elements: int, partition_size: int) -> Self:
        """
        A prefix that divides a source containing the given number of elements
        (subgraphs, files, …) into partitions that rarely exceed the given size.

        >>> n = 8192

        >>> str(Prefix.for_main_deployment(0, n))
        Traceback (most recent call last):
        ...
        ValueError: expected a positive input, got 0.0

        >>> str(Prefix.for_main_deployment(1, n))
        '/0'

        >>> cases = [-1, 0, 1, 2]

        >>> [str(Prefix.for_main_deployment(n + i, n)) for i in cases]
        ['/0', '/0', '/1', '/1']

        Sources with this many bundles are very rare, so we have a generous
        margin of error surrounding this cutoff point

        >>> m = n * 16
        >>> [str(Prefix.for_main_deployment(m + i, n)) for i in cases]
        ['/1', '/1', '/2', '/2']
        """
        partition = cls._prefix_length(num_elements, partition_size)
        return cls(common='', partition=partition)

    @classmethod
    def for_lesser_deployment(cls, num_elements: int) -> Self:
        """
        A prefix that yields an average of approximately 24 elements per
        source, using an experimentally derived heuristic formula designed to
        minimize manual adjustment of the computed common prefixes. The
        partition prefix length is always 1, even though some partitions may be
        empty, to provide test coverage for handling multiple partitions.

        >>> str(Prefix.for_lesser_deployment(0))
        Traceback (most recent call last):
        ...
        ValueError: expected a positive input, got 0.0

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
        digits = f'{num_elements - 1:x}'[::-1]
        length = cls._prefix_length(num_elements, 64)
        assert length < len(digits), num_elements
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

    def __contains__(self, partition_prefix: str) -> bool:
        """
        Same as `partition_prefix in prefix.partition_prefixes()` but more
        efficient. See also :meth:`partition_prefixes`.

        >>> p0, p1, p2 = Prefix.parse('/0'), Prefix.parse('/1'), Prefix.parse('/2')
        >>> 'a' in p0, 'a' in p1, 'a' in p2
        (False, True, False)

        >>> p1, p2, p3 = Prefix.parse('a/0'), Prefix.parse('a/1'), Prefix.parse('a/2')
        >>> 'ab' in p1, 'ab' in p2, 'ab' in p3
        (False, True, False)

        >>> 'ab' in Prefix.parse('b/1')
        False

        >>> 'ag' in Prefix.parse('a/1')
        False

        >>> 'aB' in Prefix.parse('a/1')
        False
        """
        return (
            partition_prefix.startswith(self.common)
            and len(partition_prefix) == len(self)
            and all(c in self.digits for c in partition_prefix[len(self.common):])
        )


Prefix.of_everything = Prefix.parse('/0')


@attrs.frozen(kw_only=True, order=True)
class SourceSpec(Parseable, metaclass=ABCMeta):
    """
    The name of a repository source containing bundles to index. A repository
    has at least one source. Repository plugins whose repository source names
    are structured might want to implement this abstract class. Plugins that
    have simple unstructured names may want to use :class:`SimpleSourceSpec`.
    """
    #: The name of this source. Azul assumes this to be unique per catalog.
    name: str


@attrs.frozen(kw_only=True)
class SimpleSourceSpec(SourceSpec):
    """
    Default implementation for unstructured source names.
    """

    @classmethod
    def parse(cls, spec: str) -> Self:
        """
        >>> SimpleSourceSpec.parse('https://foo.edu') # doctest: +NORMALIZE_WHITESPACE
        SimpleSourceSpec(name='https://foo.edu')
        """
        self = cls(name=spec)
        assert spec == str(self), spec
        return self

    def __str__(self) -> str:
        """
        >>> s = 'foo:bar/baz'
        >>> s == str(SimpleSourceSpec.parse(s))
        True
        """
        return self.name


@attrs.frozen(kw_only=True, order=True)
class SourceRef[SOURCE_SPEC: SourceSpec](
    DiscriminatingPolymorphicSerializableAttrs,
    DynamicPolymorphicSerializable,
    SupportsLessAndGreaterThan
):
    """
    A reference to a repository source containing bundles to index. A repository
    has at least one source. A source is primarily referenced by its ID but we
    drag the spec along to 1) avoid repeatedly looking it up and 2) ensure that
    the mapping between the two doesn't change while we index a source.

    Note to plugin implementers: Since the source ID can't be assumed to be
    globally unique, plugins should subclass this class, even if the subclass
    body is empty. Additionally, a subclass that overrides the constructor must
    keep its signature compatible_ with that of :py:meth:`SourceRef.__init__`.

    .. _compatible: https://mypy.readthedocs.io/en/stable/class_basics.html#overriding-statically-typed-methods

    >>> spec = SimpleSourceSpec(name='')
    >>> prefix = Prefix(partition=0)
    >>> sorted([
    ...     SourceRef(id='d', spec=spec, prefix=prefix),
    ...     SourceRef(id='a', spec=spec, prefix=prefix),
    ... ])
    ... # doctest: +NORMALIZE_WHITESPACE
    [SourceRef(id='a', spec=SimpleSourceSpec(name=''), prefix=Prefix(common='', partition=0)),
    SourceRef(id='d', spec=SimpleSourceSpec(name=''), prefix=Prefix(common='', partition=0))]

    """
    id: str = attrs.field(order=str.lower)
    spec: SOURCE_SPEC = attrs.field(order=False)
    prefix: Prefix | None = attrs.field(order=False)

    @classmethod
    def discriminator(cls) -> str:
        return 'type'

    @classmethod
    def spec_cls(cls) -> type[SOURCE_SPEC]:
        spec_cls = derived_type_params(cls, root=SourceRef)[SOURCE_SPEC]
        assert isinstance(spec_cls, type)
        assert issubclass(spec_cls, SourceSpec)
        return cast(type[SOURCE_SPEC], spec_cls)

    def with_prefix(self, prefix: Prefix) -> Self:
        return attrs.evolve(self, prefix=prefix)

    @classmethod
    def field_types(cls) -> FieldTypes:
        return {
            'id': pass_thru_str,
            'spec': pass_thru_str,
            'prefix': pass_thru_str,
            cls.discriminator(): pass_thru_str,
        }


@attrs.frozen(kw_only=True)
class SourceConfig(SerializableAttrs):
    """
    Configuration on how to index or mirror a specific source.
    """
    mirror: bool


@attrs.frozen(kw_only=True)
class Source(SerializableAttrs):
    ref: SourceRef
    config: SourceConfig

    def with_prefix(self, prefix: Prefix) -> Self:
        return attrs.evolve(self, ref=self.ref.with_prefix(prefix=prefix))
