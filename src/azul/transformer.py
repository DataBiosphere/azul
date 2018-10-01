from abc import ABC, abstractmethod
import itertools
from itertools import filterfalse, tee
import logging
import re
from typing import List, Mapping, Sequence, Iterable

from dataclasses import dataclass, field, asdict

from azul import config
from azul.types import JSON

logger = logging.getLogger(__name__)


@dataclass
class Bundle:
    uuid: str
    version: str
    contents: JSON = field(default_factory=dict)

    @classmethod
    def from_json(cls, json):
        return cls(**json)

    @property
    def deleted(self) -> bool:
        return self.contents.get('deleted', False)

    def delete(self):
        self.contents = {'deleted': True}


@dataclass
class ElasticSearchDocument:
    entity_type: str
    entity_id: str
    bundles: List[Bundle]
    document_type: str = "doc"
    document_version: int = 1

    @property
    def document_id(self) -> str:
        return self.entity_id

    @property
    def document_index(self) -> str:
        return config.es_index_name(self.entity_type)

    def to_source(self) -> dict:
        return {
            "entity_id": self.entity_id,
            "bundles": [asdict(b) for b in self.bundles]
        }

    @classmethod
    def from_index(cls, hit: JSON):
        source = hit["_source"]
        # FIXME: move logic to Config
        prefix, entity_type, _ = hit['_index'].split('_')
        return cls(entity_type=entity_type,
                   entity_id=source['entity_id'],
                   bundles=[Bundle(**b) for b in source['bundles']],
                   document_version=hit.get("_version", 0))

    def update_with(self, other: 'ElasticSearchDocument'):
        """
        Merge updates from another instance into this one. Typically, `self` represents a persistent document loaded
        from the index while `other` contains contributions from newly indexed bundles.
        """
        assert self._is_compatible_with(other)
        self.bundles = self._merge_bundles(self.bundles, other.bundles)
        self.document_version = self.document_version + 1

    def _is_compatible_with(self, other):
        return (self.document_id == other.document_id and
                self.document_index == other.document_index and
                self.document_type == other.document_type and
                self.entity_type == other.entity_type)

    @staticmethod
    def _merge_bundles(current: List[Bundle], updates: List[Bundle]):
        """
        >>> merge = ElasticSearchDocument._merge_bundles
        >>> B = Bundle

        Bs without a match in the other list are chosen:
        >>> merge([B(uuid='0', version='0'),                        ],
        ...       [                               B(uuid='2', version='0')])
        [Bundle(uuid='2', version='0', content={}), Bundle(uuid='0', version='0', content={})]

        If the UUID matches, the more recent bundle version is chosen:
        >>> merge([B(uuid='0', version='0'), B(uuid='2', version='1')],
        ...       [B(uuid='0', version='1'), B(uuid='2', version='0')])
        [Bundle(uuid='0', version='1', content={}), Bundle(uuid='2', version='1', content={})]

        Ties (identical UUID and version) are broken by favoring the bundle from the second argument:
        >>> merge([B(uuid='1', version='0', content={'x':1})],
        ...       [B(uuid='1', version='0', content={'x':2})])
        [Bundle(uuid='1', version='0', content={'x': 2})]

        A more complicated case:
        >>> merge([B(uuid='0', version='0'), B(uuid='1', version='0', content={'x':1}), B(uuid='2', version='0')],
        ...       [                          B(uuid='1', version='0', content={'x':2}), B(uuid='2', version='1')])
        ... # doctest: +NORMALIZE_WHITESPACE
        [Bundle(uuid='1', version='0', content={'x': 2}),
        Bundle(uuid='2', version='1', content={}),
        Bundle(uuid='0', version='0', content={})]
        """
        current_by_id = {bundle.uuid: bundle for bundle in current}
        assert len(current_by_id) == len(current)
        bundles = {}
        for update in updates:
            try:
                cur_bundle = current_by_id.pop(update.uuid)
            except KeyError:
                bundle = update
            else:
                bundle = update if update.version >= cur_bundle.version else cur_bundle
            assert bundles.setdefault(update.uuid, bundle) is bundle
        for bundle in current_by_id.values():
            assert bundles.setdefault(bundle.uuid, bundle) is bundle
        return list(bundles.values())

    def consolidate(self, others: Iterable['ElasticSearchDocument']):
        """
        Combine bundle contributions from multiple other instances into this one. All involved instances must
        represent the same metadata entity or an exception wioll be raised. The bundle contributions from all
        involved instances must be disjunctive or an exception will be raised. See `:py:methd:`update_with` for a way
        to reconcile two instances with non-disjunctive bundle contributions.
        """
        assert all(self._is_compatible_with(other) for other in others)
        bundles = {}
        for bundle in itertools.chain(self.bundles, *(other.bundles for other in others)):
            assert bundles.setdefault(bundle.uuid, bundle) is bundle
        self.bundles = list(bundles.values())

    def to_json(self):
        return asdict(self)

    @classmethod
    def from_json(cls, json: JSON):
        self = cls(**json)
        self.bundles = list(map(Bundle.from_json, self.bundles))
        return self


class Transformer(ABC):

    def __init__(self):
        super().__init__()

    @property
    def entity_name(self) -> str:
        return ""

    @staticmethod
    def partition(predicate, iterable):
        """
        Use a predicate to partition entries into false entries and
        true entries
        """
        t1, t2 = tee(iterable)
        return filterfalse(predicate, t1), filter(predicate, t2)

    @classmethod
    def get_version(cls, metadata_json: dict) -> str:
        schema_url = metadata_json["describedBy"]
        version_match = re.search(r'\d\.\d\.\d', schema_url)
        version = version_match.group()
        simple_version = version.rsplit(".", 1)
        simple_version = simple_version[0].replace('.', '_')
        return simple_version

    @abstractmethod
    def create_documents(self,
                         uuid: str,
                         version: str,
                         manifest: List[JSON],
                         metadata_files: Mapping[str, JSON]
                         ) -> Sequence[ElasticSearchDocument]:
        raise NotImplementedError()
