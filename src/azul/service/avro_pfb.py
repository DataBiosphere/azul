import bisect
from collections import (
    defaultdict,
)
from collections.abc import (
    Iterable,
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
    MutableSet,
    Self,
    Sequence,
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
    ClosedRange,
    FieldTypes,
    Nested,
    null_bool,
    null_datetime,
    null_float,
    null_int,
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
from azul.plugins.metadata.hca.indexer.transform import (
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
    'related_files': None  # None to remove field
}


def write_pfb_entities(entities: Iterable[JSON], pfb_schema: JSON, path: str):
    assert isinstance(pfb_schema, dict)
    parsed_schema = fastavro.parse_schema(pfb_schema)
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


# FIXME: Unit tests do not cover PFB handover using an AnVIL catalog
#        https://github.com/DataBiosphere/azul/issues/4606
class PFBConverter:
    """
    Converts documents from Elasticsearch into PFB entities. A document's inner
    entities correspond to PFB entities which are normalized and linked via
    Relations.
    """

    entity_type = 'files'

    def __init__(self, schema: JSON, repository_plugin: RepositoryPlugin):
        self.schema = schema
        self.repository_plugin = repository_plugin
        self._entities: dict[PFBEntity, MutableSet[PFBRelation]] = defaultdict(set)

    def add_doc(self, doc: JSON):
        """
        Add an Elasticsearch document to be transformed.
        """
        doc_copy = copy_json(doc, 'contents', self.entity_type)
        contents = doc_copy['contents']
        file_relations = set()
        for entity_type, entities in contents.items():
            # copy_json is expected to only deep copy a subset of the document
            if entity_type == self.entity_type:
                assert entities is not doc['contents'][entity_type]
            else:
                assert entities is doc['contents'][entity_type]
            entities = (e for e in entities if 'document_id' in e)
            # Sorting entities is required for deterministic output since
            # the order of the inner entities in an aggregate document is
            # tied to the order with which contributions are returned by ES
            # during aggregation, which happens to be non-deterministic.
            for entity in sorted(entities, key=itemgetter('document_id')):
                if entity_type != self.entity_type:
                    _inject_reference_handover_values(entity, doc)
                    pfb_entity = PFBEntity.from_json(name=entity_type,
                                                     object_=entity,
                                                     schema=self.schema)
                    if pfb_entity not in self._entities:
                        self._entities[pfb_entity] = set()
                    file_relations.add(PFBRelation.to_entity(pfb_entity))
        file_entity: MutableJSON = one(contents[self.entity_type])
        related_files = file_entity.pop('related_files', [])
        for entity in chain([file_entity], related_files):
            _inject_reference_handover_values(entity, doc)
            # File entities are assumed to be unique
            pfb_entity = PFBEntity.from_json(name=self.entity_type,
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


def _reversible_join(joiner: str, parts: Iterable[str]) -> str:
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
                  schema: JSON
                  ) -> Self:
        """
        Derive ID from object in a reproducible way so that we can distinguish
        entities by comparing their IDs.
        """
        cls._add_missing_fields(name, object_, schema)
        ids = object_['document_id']
        # document_id is an array unless the inner entity type matches the
        # outer entity type
        ids = sorted(ids) if isinstance(ids, list) else [ids]
        id_ = uuid5(cls.namespace_uuid, _reversible_join('_', ids))
        id_ = _reversible_join('.', map(str, (name, id_, len(ids))))
        return cls(id=id_, name=name, object=object_)

    @classmethod
    def for_replica(cls, replica: MutableJSON, schema: JSON) -> Self:
        name, object_ = replica['replica_type'], replica['contents']
        cls._add_missing_fields(name, object_, schema)
        # Note that it is possible for two distinct replicas to have the same
        # entity ID. For example, replicas representing the DUOS registration
        # of AnVIL datasets have the same ID as the replica for the dataset
        # itself. Terra appears to combine PFB entities with the same ID
        # into a single row.
        # FIXME: Improve handling of DUOS replicas
        #        https://github.com/DataBiosphere/azul/issues/6139
        return cls(id=replica['entity_id'], name=name, object=object_)

    @classmethod
    def _add_missing_fields(cls, name: str, object_: MutableJSON, schema):
        """
        Compare entities against the schema and add any fields that are missing.
        None is the default value.
        """
        if schema['type'] == 'record':
            object_schema = one(f for f in schema['fields'] if f['name'] == 'object')
            entity_schema = one(e for e in object_schema['type'] if e['name'] == name)
        elif isinstance(schema['type'], dict):
            entity_schema = schema['type']['items']
        else:
            assert False, schema
        for field in entity_schema['fields']:
            field_name, field_type = field['name'], field['type']
            if field_name not in object_:
                if isinstance(field_type, list):
                    assert 'null' in field_type, field
                    default_value = None
                elif isinstance(field_type, dict) and field_type['type'] == 'array':
                    if isinstance(field_type['items'], dict):
                        assert field_type['items']['type'] in ('record', 'array'), field
                        default_value = []
                    else:
                        assert 'null' in field_type['items'], field
                        default_value = [None]
                elif field_type == 'null':
                    default_value = None
                else:
                    assert False, field
                object_[field_name] = default_value
            if (
                isinstance(field_type, dict)
                and field_type['type'] == 'array'
                and isinstance(field_type['items'], dict)
                and field_type['items']['type'] == 'record'
            ):
                for sub_object in object_[field_name]:
                    cls._add_missing_fields(name=field_name,
                                            object_=sub_object,
                                            schema=field)

    def to_json(self, relations: Iterable['PFBRelation']) -> JSON:
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
    def to_entity(cls, entity: PFBEntity) -> Self:
        return cls(dst_id=entity.id, dst_name=entity.name)


def pfb_metadata_entity(entity_types: Iterable[str],
                        links: bool = True
                        ) -> MutableJSON:
    """
    The Metadata entity encodes the possible relationships between tables.

    Unfortunately Terra does not display the relations between the nodes.
    """
    return {
        'id': None,
        'name': 'Metadata',
        'object': {
            'nodes': [
                {
                    'name': entity_type,
                    'ontology_reference': '',
                    'values': {},
                    'links': [] if not links or entity_type == 'files' else [{
                        'multiplicity': 'MANY_TO_MANY',
                        'dst': 'files',
                        'name': 'files'
                    }],
                    'properties': []
                } for entity_type in entity_types
            ],
            'misc': {}
        }
    }


def pfb_schema_from_field_types(field_types: FieldTypes) -> JSON:
    field_types = _inject_reference_handover_columns(field_types)
    entity_schemas = (
        {
            'name': entity_type,
            'type': 'record',
            'fields': list(_entity_schema_recursive(field_type, entity_type))
        }
        for entity_type, field_type in field_types.items()
        # We skip primitive top-level fields like total_estimated_cells
        if isinstance(field_type, dict)
    )
    return avro_pfb_schema(entity_schemas)


def pfb_schema_from_replicas(replicas: Iterable[JSON]
                             ) -> tuple[Sequence[str], JSON]:
    schemas_by_replica_type = {}
    for replica in replicas:
        replica_type, replica_contents = replica['replica_type'], replica['contents']
        _update_replica_schema(schema=schemas_by_replica_type,
                               path=(replica_type,),
                               key=replica_type,
                               value=replica_contents)
    schemas_by_replica_type = sorted(schemas_by_replica_type.items())
    keys, values = zip(*schemas_by_replica_type)
    return keys, avro_pfb_schema(values)


def avro_pfb_schema(azul_avro_schema: Iterable[JSON]) -> JSON:
    """
    The boilerplate Avro schema that comprises a PFB's schema is returned in
    this JSON literal below. This schema was copied from

    https://github.com/uc-cdis/pypfb/blob/1497bf50e5c85201f6bad9ca69616138b17b8c77/src/pfb/writer.py#L85

    :param azul_avro_schema: The parts of the schema describe the custom tables
                             we insert into the PFB

    :return: The complete and valid Avro schema
    """
    return {
        'type': 'record',
        'name': 'Entity',
        'fields': [
            {
                'name': 'id',
                'type': ['null', 'string'],
                'default': None
            },
            {
                'name': 'name',
                'type': 'string'
            },
            {
                'name': 'object',
                'type': [
                    {
                        'type': 'record',
                        'name': 'Metadata',
                        'fields': [
                            {
                                'name': 'nodes',
                                'type': {
                                    'type': 'array',
                                    'items': {
                                        'type': 'record',
                                        'name': 'Node',
                                        'fields': [
                                            {
                                                'name': 'name',
                                                'type': 'string'
                                            },
                                            {
                                                'name': 'ontology_reference',
                                                'type': 'string',
                                            },
                                            {
                                                'name': 'values',
                                                'type': {
                                                    'type': 'map',
                                                    'values': 'string',
                                                },
                                            },
                                            {
                                                'name': 'links',
                                                'type': {
                                                    'type': 'array',
                                                    'items': {
                                                        'type': 'record',
                                                        'name': 'Link',
                                                        'fields': [
                                                            {
                                                                'name': 'multiplicity',
                                                                'type': {
                                                                    'type': 'enum',
                                                                    'name': 'Multiplicity',
                                                                    'symbols': [
                                                                        'ONE_TO_ONE',
                                                                        'ONE_TO_MANY',
                                                                        'MANY_TO_ONE',
                                                                        'MANY_TO_MANY',
                                                                    ],
                                                                },
                                                            },
                                                            {
                                                                'name': 'dst',
                                                                'type': 'string',
                                                            },
                                                            {
                                                                'name': 'name',
                                                                'type': 'string',
                                                            },
                                                        ],
                                                    },
                                                },
                                            },
                                            {
                                                'name': 'properties',
                                                'type': {
                                                    'type': 'array',
                                                    'items': {
                                                        'type': 'record',
                                                        'name': 'Property',
                                                        'fields': [
                                                            {
                                                                'name': 'name',
                                                                'type': 'string',
                                                            },
                                                            {
                                                                'name': 'ontology_reference',
                                                                'type': 'string',
                                                            },
                                                            {
                                                                'name': 'values',
                                                                'type': {
                                                                    'type': 'map',
                                                                    'values': 'string',
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
                                'name': 'misc',
                                'type': {
                                    'type': 'map',
                                    'values': 'string'
                                },
                            },
                        ],
                    },
                    *azul_avro_schema
                ]
            },
            {
                'name': 'relations',
                'type': {
                    'type': 'array',
                    'items': {
                        'type': 'record',
                        'name': 'Relation',
                        'fields': [
                            {
                                'name': 'dst_id',
                                'type': 'string'
                            },
                            {
                                'name': 'dst_name',
                                'type': 'string'
                            },
                        ],
                    },
                },
                'default': [],
            },
        ],
    }


def _inject_reference_handover_columns(field_types: FieldTypes) -> FieldTypes:
    return {
        entity_type: (
            dict(fields, datarepo_row_id=null_str, source_datarepo_snapshot_id=null_str)
            if isinstance(fields, dict) and 'source_datarepo_row_ids' in fields
            else fields
        )
        for entity_type, fields in field_types.items()
    }


def _inject_reference_handover_values(entity: MutableJSON, doc: JSON):
    if 'source_datarepo_row_ids' in entity:
        entity['datarepo_row_id'] = entity['document_id']
        entity['source_datarepo_snapshot_id'] = one(doc['sources'])['id']


# FIXME: It's not obvious as to why these are union types. Explain or change.
#        https://github.com/DataBiosphere/azul/issues/4094

# FIXME: It seems that these are just all primitive types, it just so happens
#        that all of the primitive field types types are nullable
#        https://github.com/DataBiosphere/azul/issues/4094

_json_to_pfb_types = {
    bool: 'boolean',
    float: 'double',
    int: 'long',
    str: 'string'
}

_nullable_to_pfb_types = {
    null_bool: ['null', 'boolean'],
    null_float: ['null', 'double'],
    null_int: ['null', 'long'],
    null_str: ['null', 'string'],
    null_datetime: ['null', 'string'],
}


def _entity_schema_recursive(field_types: FieldTypes,
                             *path: str
                             ) -> Iterable[JSON]:
    for field_name, field_type in field_types.items():
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

        name_fields = {'name': field_name}
        if path:
            namespace = '.'.join(path)
            qualified_name = namespace + '.' + field_name
            name_fields['namespace'] = namespace
        else:
            qualified_name = field_name

        if isinstance(field_type, dict):
            yield {
                **name_fields,
                'type': {
                    # This is always an array, even if singleton is passed in
                    'type': 'array',
                    'items': {
                        'name': qualified_name,
                        'type': 'record',
                        'fields': list(_entity_schema_recursive(field_type, *path, field_name))
                    }
                }
            }
        elif field_type in _nullable_to_pfb_types:
            # Exceptions are fields that do not become lists during aggregation
            field_exceptions = (
                'donor_count',
                'estimated_cell_count',
                'total_estimated_cells',
                'total_estimated_cells_redundant',
                'source_datarepo_snapshot_id',
            )
            path_exceptions = (
                ('projects', 'accessions'),
                ('projects', 'tissue_atlas')
            )
            # FIXME: The first term is not self-explanatory
            #        https://github.com/DataBiosphere/azul/issues/4094
            if (
                path[0] == 'files' and not plural
                or field_name in field_exceptions
                or path in path_exceptions
            ):
                yield {
                    **name_fields,
                    'type': _nullable_to_pfb_types[field_type],
                }
            else:
                yield {
                    **name_fields,
                    'type': {
                        'type': 'array',
                        'items': _nullable_to_pfb_types[field_type],
                    }
                }
        elif field_type is pass_thru_uuid4:
            yield {
                **name_fields,
                'type': ['string'],
                'logicalType': 'UUID'
            }
        elif isinstance(field_type, ClosedRange):
            yield {
                **name_fields,
                'type': {
                    'type': 'array',
                    'items': {
                        'type': 'array',
                        'items': _json_to_pfb_types[field_type.ends_type.native_type]
                    }
                }
            }
        # FIXME: Nested is handled so much more elegantly. See if we can have
        #        ValueAndUnit inherit Nested.
        #        https://github.com/DataBiosphere/azul/issues/4094
        elif field_type is value_and_unit:
            yield {
                **name_fields,
                'type': {
                    'type': 'array',
                    'items': [
                        'null',
                        {
                            # FIXME: Why do we need to repeat `name` and `namespace`
                            #        with the same values at two different depths?
                            #        https://github.com/DataBiosphere/azul/issues/4094
                            'name': qualified_name,
                            'type': 'record',
                            'fields': [
                                {
                                    'name': name,
                                    'namespace': qualified_name,
                                    # Although, not technically a null_str, it's effectively the same
                                    'type': _nullable_to_pfb_types[null_str]
                                }
                                for name in ('value', 'unit')
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


def _sort_pfb_union(schema: str | dict) -> str:
    if isinstance(schema, str):
        return schema
    else:
        return schema['type']


class SchemaUpdateException(Exception):
    pass


def _update_replica_schema(*,
                           schema: MutableJSON,
                           path: tuple[str, ...],
                           key: str,
                           value: AnyMutableJSON):
    """
    Update in place a (part of an) existing PFB schema to ensure that it
    accommodates a given (part of a) JSON document. The schema will only ever
    expand, so after updating it will describe a superset of the documents that
    it described pre-update. Starting from an empty schema, repeatedly calling
    this function allows us to discover a general schema for a series of
    documents of unknown shape.

    :param schema: a part of a PFB schema. It may be empty.

    :param path: the series of field names that locate `schema` within its
                 top-level parent schema. The first entry should be the name of
                 the underlying PFB entity's record type.

    :param key: the key within `schema` whose associated value will be updated
                to describe `value`. This is the only part of `schema` that may
                be mutated.

    :param value: a part of a PFB entity.
    """
    try:
        old_type = schema[key]
    except KeyError:
        schema[key] = _new_replica_schema(path=path, value=value)
    else:
        if isinstance(old_type, list):
            _update_replica_schema_union(schema=schema, path=path, key=key, value=value)
        else:
            if value is None and old_type == 'null':
                pass
            elif (isinstance(value, list)
                  and isinstance(old_type, dict) and old_type['type'] == 'array'):
                for v in value:
                    _update_replica_schema_union(schema=old_type,
                                                 path=path,
                                                 key='items',
                                                 value=v)
            elif (isinstance(value, dict)
                  and isinstance(old_type, dict) and old_type['type'] == 'record'):
                old_fields = {field['name']: field for field in old_type['fields']}
                for k in value.keys() | old_fields.keys():
                    try:
                        field = old_fields[k]
                    except KeyError:
                        field = {
                            'name': k,
                            'namespace': '.'.join(path),
                            'type': 'null'
                        }
                        bisect.insort(old_type['fields'], field, key=itemgetter('name'))
                        new_value = value[k]
                    else:
                        new_value = value.get(k)
                    _update_replica_schema_union(schema=field,
                                                 path=(*path, k),
                                                 key='type',
                                                 value=new_value)
            else:
                try:
                    new_type = _json_to_pfb_types[type(value)]
                except KeyError:
                    raise SchemaUpdateException
                else:
                    if new_type != old_type:
                        raise SchemaUpdateException


def _update_replica_schema_union(*,
                                 schema: MutableJSON,
                                 path: tuple[str, ...],
                                 key: str,
                                 value: AnyMutableJSON):
    old_type = schema[key]
    if not isinstance(old_type, list):
        old_type = [old_type]
    for union_member in old_type:
        try:
            _update_replica_schema(schema={key: union_member},
                                   path=path,
                                   key=key,
                                   value=value)
        except SchemaUpdateException:
            continue
        else:
            break
    else:
        new_type = _new_replica_schema(path=path, value=value)
        if old_type:
            bisect.insort(old_type, new_type, key=_sort_pfb_union)
        else:
            old_type = new_type
        schema[key] = old_type


def _new_replica_schema(*,
                        path: tuple[str, ...],
                        value: AnyJSON,
                        ) -> AnyMutableJSON:
    """
    Create a part of a PFB schema to describe a part of a PFB entity represented
    as a JSON document.

    :param path: the location of `value` within the root document as a series
                 of keys. The first key should be the name of the underlying PFB
                 entity's type within the schema.

    :param value: a part of a PFB entity.

    :return: JSON describing the contents of `value` as a part of PFB schema.
    """
    if value is None:
        result = 'null'
    elif isinstance(value, list):
        # Empty list indicates "no type" (emtpy union). This will be replaced
        # with an actual type unless we never encounter a non-empty array.
        result = {'type': 'array', 'items': []}
        for v in value:
            _update_replica_schema(schema=result,
                                   path=path,
                                   key='items',
                                   value=v)
    elif isinstance(value, dict):
        name = '.'.join(path)
        result = {
            'name': name,
            'type': 'record',
            'fields': [
                {
                    'name': k,
                    'namespace': name,
                    'type': _new_replica_schema(path=(*path, k), value=v)
                }
                for k, v in sorted(value.items())
            ]
        }
    else:
        result = _json_to_pfb_types[type(value)]
    return result
