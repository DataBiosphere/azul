from collections import (
    defaultdict,
)
from itertools import (
    chain,
)
import logging
from operator import (
    attrgetter,
    itemgetter,
)
from typing import (
    ClassVar,
    Iterable,
    MutableMapping,
    MutableSet,
    cast,
)
from uuid import (
    UUID,
    uuid5,
)

import attr
import fastavro
from fastavro.validation import (
    ValidationError,
)
from more_itertools import (
    one,
)

from azul import (
    config,
    reject,
)
from azul.indexer.document import (
    FieldTypes,
    null_bool,
    null_datetime,
    null_float,
    null_int,
    null_int_sum_sort,
    null_str,
    pass_thru_int,
    pass_thru_json,
)
from azul.json import (
    copy_json,
)
from azul.plugins import (
    RepositoryPlugin,
)
from azul.plugins.metadata.hca.transform import (
    Nested,
    pass_thru_uuid4,
    value_and_unit,
)
from azul.types import (
    AnyJSON,
    AnyMutableJSON,
    JSON,
    MutableJSON,
)

log = logging.getLogger(__name__)

renamed_fields = {
    'drs_path': 'drs_uri',
    'related_files': None  # None to remove field
}


def write_pfb_entities(entities: Iterable[JSON], pfb_schema: JSON, path: str):
    # fastavro doesn't know about our JSON type, hence the cast
    parsed_schema = fastavro.parse_schema(cast(dict, pfb_schema))
    with open(path, 'w+b') as fh:
        # Writing the entities one at a time is ~2.5 slower, but makes it clear
        # which entities fail, which is useful for debugging.
        if config.debug > 1:
            log.info('Writing PFB entities individually')
            for entity in entities:
                try:
                    fastavro.writer(fh, parsed_schema, [entity], validator=True)
                except ValidationError:
                    log.error('Failed to write Avro entity: %r', entity)
                    raise
        else:
            fastavro.writer(fh, parsed_schema, entities, validator=True)


class PFBConverter:
    """
    Converts documents from Elasticsearch into PFB entities. A document's inner
    entities correspond to PFB entities which are normalized and linked via
    Relations.
    """

    def __init__(self, schema: JSON, repository_plugin: RepositoryPlugin):
        self.schema = schema
        self.repository_plugin = repository_plugin
        self._entities: MutableMapping[PFBEntity,
                                       MutableSet[PFBRelation]] = defaultdict(set)

    def add_doc(self, doc: JSON):
        """
        Add an Elasticsearch document to be transformed.
        """
        doc_copy = copy_json(doc, 'contents', 'files')
        contents = doc_copy['contents']
        assert contents['projects'] is doc['contents']['projects']
        assert contents['files'] is not doc['contents']['files']
        file_relations = set()
        for entity_type, entities in contents.items():
            # FIXME: Protocol entities lack document ID so we skip for now
            #        https://github.com/DataBiosphere/azul/issues/3084
            entities = (e for e in entities if 'document_id' in e)
            # Sorting entities is required for deterministic output since
            # the order of the inner entities in an aggregate document is
            # tied to the order with which contributions are returned by ES
            # during aggregation, which happens to be non-deterministic.
            for entity in sorted(entities, key=itemgetter('document_id')):
                if entity_type != 'files':
                    pfb_entity = PFBEntity.from_json(name=entity_type,
                                                     object_=entity,
                                                     schema=self.schema)
                    if pfb_entity not in self._entities:
                        self._entities[pfb_entity] = set()
                    file_relations.add(PFBRelation.to_entity(pfb_entity))
        file_entity: MutableJSON = one(contents['files'])
        related_files = file_entity.pop('related_files', [])
        for entity in chain([file_entity], related_files):
            if entity != file_entity:
                # Replace the file entity with a related file
                contents['files'][:] = entity
            entity['drs_uri'] = self.repository_plugin.drs_uri(entity.pop('drs_path'))
            # File entities are assumed to be unique
            pfb_entity = PFBEntity.from_json(name='files',
                                             object_=entity,
                                             schema=self.schema)
            assert pfb_entity not in self._entities
            # Terra streams PFBs and requires entities be defined before they are
            # referenced. Thus we add the file entity after all the entities
            # it relates to.
            self._entities[pfb_entity] = file_relations

    def entities(self) -> Iterable[JSON]:
        for entity, relations in self._entities.items():
            # Sort relations to make entities consistent for easy diffing
            yield entity.to_json(sorted(relations, key=attrgetter('dst_name', 'dst_id')))


def _reversible_join(joiner: str, parts: Iterable[str]):
    parts = list(parts)
    reject(any(joiner in part for part in parts), parts)
    return joiner.join(parts)


@attr.s(auto_attribs=True, frozen=True, kw_only=True)
class PFBEntity:
    """
    Python representation of the PFB data object. Attribute names conform to the
    PFB spec (which simplifies serialization).
    """
    id: str
    name: str
    object: MutableJSON = attr.ib(eq=False)
    namespace_uuid: ClassVar[UUID] = UUID('bc93372b-9218-4f0e-b64e-6f3b339687d6')

    def __attrs_post_init__(self):
        reject(len(self.id) > 254, 'Terra requires IDs be no longer than 254 chars', )

    @classmethod
    def from_json(cls,
                  name: str,
                  object_: MutableJSON,
                  schema: JSON) -> 'PFBEntity':
        """
        Derive ID from object in a reproducible way so that we can distinguish
        entities by comparing their IDs.
        """
        cls._add_missing_fields(name, object_, schema)
        object_ = cls._replace_null_with_empty_string(object_)
        # For files, document_id is not unique (because of related_files), but
        # uuid is.
        ids = [object_['uuid']] if name == 'files' else sorted(object_['document_id'])
        id_ = uuid5(cls.namespace_uuid, _reversible_join('_', ids))
        id_ = _reversible_join('.', map(str, (name, id_, len(ids))))
        return cls(id=id_, name=name, object=object_)

    @classmethod
    def _add_missing_fields(cls, name: str, object_: MutableJSON, schema):
        """
        Compare entities against the schema and add any fields that are missing.

        None is the default value, but because of https://github.com/DataBiosphere/azul/issues/2370
        this isn't currently reflected in the schema.
        """
        if schema['type'] == 'record':
            object_schema = one(f for f in schema['fields'] if f['name'] == 'object')
            entity_schema = one(e for e in object_schema['type'] if e['name'] == name)
        elif isinstance(schema['type'], dict):
            entity_schema = schema['type']['items']
        else:
            assert False, schema
        for field in entity_schema['fields']:
            field_name = field['name']
            if field_name not in object_:
                if isinstance(field['type'], list):
                    # FIXME: Change 'string' to 'null'
                    #        https://github.com/DataBiosphere/azul/issues/2462
                    assert 'string' in field['type'] or 'null' in field['type'], field
                    default_value = None
                elif field['type']['type'] == 'array':
                    if isinstance(field['type']['items'], dict):
                        assert field['type']['items']['type'] == 'record', field
                        default_value = []
                    else:
                        # FIXME: Change 'string' to 'null'
                        #        https://github.com/DataBiosphere/azul/issues/2462
                        assert 'string' in field['type']['items'], field
                        default_value = [None]
                else:
                    assert False, field
                object_[field_name] = default_value
            if (
                isinstance(field['type'], dict)
                and field['type']['type'] == 'array'
                and isinstance(field['type']['items'], dict)
            ):
                for sub_object in object_[field_name]:
                    cls._add_missing_fields(name=field_name,
                                            object_=sub_object,
                                            schema=field)

    @classmethod
    def _replace_null_with_empty_string(cls, object_json: AnyJSON) -> AnyMutableJSON:
        # FIXME: remove with https://github.com/DataBiosphere/azul/issues/2462
        if object_json is None:
            return ''
        elif isinstance(object_json, dict):
            return {
                k: cls._replace_null_with_empty_string(v)
                for k, v in object_json.items()
            }
        elif isinstance(object_json, list):
            return [
                cls._replace_null_with_empty_string(item)
                for item in object_json
            ]
        else:
            return object_json

    def to_json(self, relations: Iterable['PFBRelation']):
        return {
            'id': self.id,
            'name': self.name,
            # https://fastavro.readthedocs.io/en/latest/writer.html#using-the-tuple-notation-to-specify-which-branch-of-a-union-to-take
            'object': (self.name, self.object),
            'relations': [attr.asdict(relation) for relation in relations]
        }


@attr.s(auto_attribs=True, frozen=True, kw_only=True)
class PFBRelation:
    dst_id: str
    # A more appropriate attribute name would be dst_type, but we stick with
    # 'dst_name' to conform to PFB spec
    dst_name: str

    @classmethod
    def to_entity(cls, entity: PFBEntity):
        return cls(dst_id=entity.id, dst_name=entity.name)


def pfb_metadata_entity(field_types: FieldTypes):
    """
    The Metadata entity encodes the possible relationships between tables.

    Unfortunately Terra does not display the relations between the nodes.
    """
    return {
        "id": None,
        "name": "Metadata",
        "object": {
            "nodes": [
                {
                    "name": field_type,
                    "ontology_reference": "",
                    "values": {},
                    "links": [] if field_type == 'files' else [{
                        "multiplicity": "MANY_TO_MANY",
                        "dst": "files",
                        "name": "files"
                    }],
                    "properties": []
                } for field_type in field_types
            ],
            "misc": {}
        }
    }


def pfb_schema_from_field_types(field_types: FieldTypes) -> JSON:
    entity_schemas = (
        {
            "name": entity_type,
            "namespace": "",
            "type": "record",
            "fields": list(_entity_schema_recursive(field_type, entity_type))
        }
        for entity_type, field_type in field_types.items()
        # We skip primitive top-level fields like total_estimated_cells
        if isinstance(field_type, dict)
    )
    return _avro_pfb_schema(entity_schemas)


def _avro_pfb_schema(azul_avro_schema: Iterable[JSON]) -> JSON:
    """
    The boilerplate Avro schema that comprises a PFB's schema is returned in
    this JSON literal below. This schema was copied from
    https://github.com/uc-cdis/pypfb/blob/1497bf50e5c85201f6bad9ca69616138b17b8c77/src/pfb/writer.py#L85

    :param azul_avro_schema: The parts of the schema describe the custom tables
        we insert into the PFB
    :return: The complete and valid Avro schema
    """
    return {
        "type": "record",
        "name": "Entity",
        "fields": [
            {"name": "id", "type": ["null", "string"], "default": None},
            {"name": "name", "type": "string"},
            {
                "name": "object",
                "type": [
                    {
                        "type": "record",
                        "name": "Metadata",
                        "fields": [
                            {
                                "name": "nodes",
                                "type": {
                                    "type": "array",
                                    "items": {
                                        "type": "record",
                                        "name": "Node",
                                        "fields": [
                                            {"name": "name", "type": "string"},
                                            {
                                                "name": "ontology_reference",
                                                "type": "string",
                                            },
                                            {
                                                "name": "values",
                                                "type": {
                                                    "type": "map",
                                                    "values": "string",
                                                },
                                            },
                                            {
                                                "name": "links",
                                                "type": {
                                                    "type": "array",
                                                    "items": {
                                                        "type": "record",
                                                        "name": "Link",
                                                        "fields": [
                                                            {
                                                                "name": "multiplicity",
                                                                "type": {
                                                                    "type": "enum",
                                                                    "name": "Multiplicity",
                                                                    "symbols": [
                                                                        "ONE_TO_ONE",
                                                                        "ONE_TO_MANY",
                                                                        "MANY_TO_ONE",
                                                                        "MANY_TO_MANY",
                                                                    ],
                                                                },
                                                            },
                                                            {
                                                                "name": "dst",
                                                                "type": "string",
                                                            },
                                                            {
                                                                "name": "name",
                                                                "type": "string",
                                                            },
                                                        ],
                                                    },
                                                },
                                            },
                                            {
                                                "name": "properties",
                                                "type": {
                                                    "type": "array",
                                                    "items": {
                                                        "type": "record",
                                                        "name": "Property",
                                                        "fields": [
                                                            {
                                                                "name": "name",
                                                                "type": "string",
                                                            },
                                                            {
                                                                "name": "ontology_reference",
                                                                "type": "string",
                                                            },
                                                            {
                                                                "name": "values",
                                                                "type": {
                                                                    "type": "map",
                                                                    "values": "string",
                                                                },
                                                            },
                                                        ],
                                                    },
                                                },
                                            },
                                        ],
                                    },
                                },
                            },
                            {
                                "name": "misc",
                                "type": {"type": "map", "values": "string"},
                            },
                        ],
                    },
                    *azul_avro_schema
                ]
            },
            {
                "name": "relations",
                "type": {
                    "type": "array",
                    "items": {
                        "type": "record",
                        "name": "Relation",
                        "fields": [
                            {"name": "dst_id", "type": "string"},
                            {"name": "dst_name", "type": "string"},
                        ],
                    },
                },
                "default": [],
            },
        ],
    }


_nullable_to_pfb_types = {
    null_bool: ['string', 'boolean'],
    null_float: ['string', 'double'],  # Not present in current field_types
    null_int: ['string', 'long'],
    null_str: ['string'],
    null_int_sum_sort: ['string', 'long'],
    null_datetime: ['string'],
}


def _entity_schema_recursive(field_types: FieldTypes,
                             *path: str) -> Iterable[JSON]:
    for field_name, field_type in field_types.items():
        namespace = '.'.join(path)
        plural = isinstance(field_type, list)
        if plural:
            field_type = one(field_type)
        try:
            new_field_name = renamed_fields[field_name]
        except KeyError:
            pass
        else:
            if new_field_name is None:
                break  # to not include this field in the schema
            else:
                field_name = new_field_name
        if isinstance(field_type, Nested):
            field_type = field_type.properties
        if isinstance(field_type, dict):
            yield {
                "name": field_name,
                "namespace": namespace,
                "type": {
                    # This is always an array, even if singleton is passed in
                    "type": "array",
                    "items": {
                        "name": field_name,
                        "namespace": namespace,
                        "type": "record",
                        "fields": list(_entity_schema_recursive(field_type, *path, field_name))
                    }
                }
            }
        elif field_type in _nullable_to_pfb_types:
            # Exceptions are fields that do not become lists during aggregation
            exceptions = (
                'donor_count',
                'estimated_cell_count',
                'submission_date',
                'total_estimated_cells',
                'update_date',
                'last_modified_date',
            )
            if path[0] == 'files' and not plural or field_name in exceptions:
                yield {
                    "name": field_name,
                    "namespace": namespace,
                    "type": list(_nullable_to_pfb_types[field_type]),
                }
            else:
                yield {
                    "name": field_name,
                    "namespace": namespace,
                    "type": {
                        "type": "array",
                        "items": list(_nullable_to_pfb_types[field_type]),
                    }
                }
        elif field_type is pass_thru_uuid4:
            yield {
                "name": field_name,
                "namespace": namespace,
                "default": None,
                "type": ["string"],
                "logicalType": "UUID"
            }
        elif field_type is value_and_unit:
            yield {
                "name": field_name,
                "namespace": namespace,
                "type": {
                    "name": field_name,
                    "namespace": namespace,
                    "type": "array",
                    "items": [
                        # FIXME: Change 'string' to 'null'
                        #        https://github.com/DataBiosphere/azul/issues/2462
                        "string",
                        {
                            "name": field_name,
                            "namespace": namespace,
                            "type": "record",
                            "fields": [
                                {
                                    "name": name,
                                    "namespace": namespace + '.' + field_name,
                                    # Although, not technically a null_str, it's effectively the same
                                    "type": _nullable_to_pfb_types[null_str]
                                } for name in ('value', 'unit')
                            ]
                        }
                    ]
                }
            }
        elif field_type in (pass_thru_json, pass_thru_int):
            # Pass thru types are used only for aggregation and are excluded
            # from actual hits
            pass
        else:
            assert False, field_type
