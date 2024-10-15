from collections import (
    defaultdict,
)
from concurrent.futures import (
    ThreadPoolExecutor,
)
from itertools import (
    islice,
)
import json
import logging
from operator import (
    itemgetter,
)
from typing import (
    Any,
    ClassVar,
    Iterable,
    cast,
)

import attr
from furl import (
    furl,
)
from more_itertools import (
    one,
)

from azul import (
    JSON,
    RequirementError,
    config,
    require,
)
from azul.bigquery import (
    BigQueryRow,
    backtick,
)
from azul.drs import (
    RegularDRSURI,
)
from azul.indexer import (
    BundleFQID,
)
from azul.indexer.document import (
    EntityID,
    EntityReference,
    EntityType,
)
from azul.plugins.metadata.hca.bundle import (
    HCABundle,
)
from azul.plugins.repository.tdr import (
    TDRBundle,
    TDRBundleFQID,
    TDRPlugin,
)
from azul.strings import (
    single_quote as sq,
)
from azul.terra import (
    TDRSourceRef,
    TDRSourceSpec,
)
from azul.types import (
    JSONs,
    is_optional,
)

log = logging.getLogger(__name__)

Entities = set[EntityReference]
EntitiesByType = dict[EntityType, set[EntityID]]


@attr.s(frozen=True, auto_attribs=True)
class Links:
    project: EntityReference
    processes: Entities = attr.Factory(set)
    protocols: Entities = attr.Factory(set)
    inputs: Entities = attr.Factory(set)
    outputs: Entities = attr.Factory(set)
    supplementary_files: Entities = attr.Factory(set)

    @classmethod
    def from_json(cls, project: EntityReference, links_json: JSON) -> 'Links':
        """
        A `links.json` file, in a more accessible form.

        :param links_json: The contents of a `links.json` file.

        :param project: A reference to the project the given `links.json`
                        belongs to.
        """
        self = cls(project)
        for link in cast(JSONs, links_json['links']):
            link_type = link['link_type']
            if link_type == 'process_link':
                self.processes.add(EntityReference(entity_type=link['process_type'],
                                                   entity_id=link['process_id']))
                for category in ('input', 'output', 'protocol'):
                    plural = category + 's'
                    target = getattr(self, plural)
                    for entity in cast(JSONs, link[plural]):
                        target.add(EntityReference(entity_type=entity[category + '_type'],
                                                   entity_id=entity[category + '_id']))
            elif link_type == 'supplementary_file_link':
                associate = EntityReference(entity_type=link['entity']['entity_type'],
                                            entity_id=link['entity']['entity_id'])
                # For MVP, only project entities can have associated supplementary files.
                require(associate == project,
                        'Supplementary file must be associated with the current project',
                        project, associate)
                for entity in cast(JSONs, link['files']):
                    self.supplementary_files.add(
                        EntityReference(entity_type='supplementary_file',
                                        entity_id=entity['file_id']))
            else:
                raise RequirementError('Unexpected link_type', link_type)
        return self

    def all_entities(self) -> Entities:
        return set.union(*(value if isinstance(value, set) else {value}
                           for field, value in attr.asdict(self, recurse=False).items()))

    def dangling_inputs(self) -> Entities:
        return {
            input_
            for input_ in self.inputs
            if input_.entity_type.endswith('_file') and not (
                input_ in self.outputs or
                input_ in self.supplementary_files
            )
        }


@attr.s(auto_attribs=True, kw_only=True, frozen=True)
class Checksums:
    crc32c: str
    sha1: str | None = None
    sha256: str
    s3_etag: str | None = None

    def to_json(self) -> dict[str, str]:
        """
        >>> Checksums(crc32c='a', sha1='b', sha256='c', s3_etag=None).to_json()
        {'crc32c': 'a', 'sha1': 'b', 'sha256': 'c'}
        """
        return {k: v for k, v in attr.asdict(self).items() if v is not None}

    @classmethod
    def from_json(cls, json: JSON) -> 'Checksums':
        """
        >>> Checksums.from_json({'crc32c': 'a', 'sha256': 'c'})
        Checksums(crc32c='a', sha1=None, sha256='c', s3_etag=None)

        >>> Checksums.from_json({'crc32c': 'a', 'sha1':'b', 'sha256': 'c', 's3_etag': 'd'})
        Checksums(crc32c='a', sha1='b', sha256='c', s3_etag='d')

        >>> Checksums.from_json({'crc32c': 'a'})
        Traceback (most recent call last):
            ...
        ValueError: ('JSON property cannot be absent or null', 'sha256')
        """

        def extract_field(field: attr.Attribute) -> tuple[str, Any]:
            value = json.get(field.name)
            if value is None and not is_optional(field.type):
                raise ValueError('JSON property cannot be absent or null', field.name)
            return field.name, value

        return cls(**dict(map(extract_field, attr.fields(cls))))


class TDRHCABundle(HCABundle[TDRBundleFQID], TDRBundle):

    @classmethod
    def canning_qualifier(cls) -> str:
        return super().canning_qualifier() + '.hca'

    def add_entity(self,
                   *,
                   entity: EntityReference,
                   row: BigQueryRow,
                   is_stitched: bool
                   ) -> None:
        if is_stitched:
            self.stitched.add(entity.entity_id)
        if entity.entity_type.endswith('_file'):
            descriptor = json.loads(row['descriptor'])
            self._add_manifest_entry(entity,
                                     name=row['file_name'],
                                     uuid=descriptor['file_id'],
                                     version=descriptor['file_version'],
                                     size=descriptor['size'],
                                     content_type=descriptor['content_type'],
                                     dcp_type='data',
                                     checksums=Checksums.from_json(descriptor),
                                     drs_uri=self._parse_drs_uri(row['file_id'], descriptor))
        content = row['content']
        self.metadata[str(entity)] = (json.loads(content)
                                      if isinstance(content, str)
                                      else content)

    metadata_columns: ClassVar[set[str]] = {
        'content'
    }

    data_columns: ClassVar[set[str]] = metadata_columns | {
        'descriptor',
        'JSON_EXTRACT_SCALAR(content, "$.file_core.file_name") AS file_name',
        'file_id'
    }

    # `links_id` is omitted for consistency since the other sets do not include
    # the primary key
    links_columns: ClassVar[set[str]] = metadata_columns | {
        'project_id'
    }

    _suffix = 'tdr.'

    def _add_manifest_entry(self,
                            entity: EntityReference,
                            *,
                            name: str,
                            uuid: str,
                            version: str,
                            size: int,
                            content_type: str,
                            dcp_type: str,
                            checksums: Checksums | None = None,
                            drs_uri: str | None = None) -> None:
        self.manifest[str(entity)] = {
            'name': name,
            'uuid': uuid,
            'version': version,
            'content-type': f'{content_type}; dcp-type={dcp_type}',
            'size': size,
            **(
                {
                    'indexed': True,
                    'crc32c': '',
                    'sha256': ''
                } if checksums is None else {
                    'indexed': False,
                    'drs_uri': drs_uri,
                    **checksums.to_json()
                }
            )
        }

    def _parse_drs_uri(self,
                       file_id: str | None,
                       descriptor: JSON
                       ) -> str | None:
        if file_id is None:
            try:
                external_drs_uri = descriptor['drs_uri']
            except KeyError:
                raise RequirementError('`file_id` is null and `drs_uri` '
                                       'is not set in file descriptor', descriptor)
            else:
                # FIXME: Support non-null DRS URIs in file descriptors
                #        https://github.com/DataBiosphere/azul/issues/3631
                if external_drs_uri is not None:
                    log.warning('Non-null `drs_uri` in file descriptor (%s)', external_drs_uri)
                    external_drs_uri = None
                return external_drs_uri
        else:
            # This requirement prevent mismatches in the DRS domain, and ensures
            # that changes to the column syntax don't go undetected.
            parsed = RegularDRSURI.parse(file_id)
            require(parsed.uri.netloc == config.tdr_service_url.netloc)
            return file_id


class Plugin(TDRPlugin[TDRHCABundle, TDRSourceSpec, TDRSourceRef, TDRBundleFQID]):

    def _count_subgraphs(self, source: TDRSourceSpec) -> int:
        rows = self._run_sql(f'''
            SELECT COUNT(*) AS count
            FROM {backtick(self._full_table_name(source, 'links'))}
        ''')
        return one(rows)['count']

    def _list_bundles(self,
                      source: TDRSourceRef,
                      prefix: str
                      ) -> list[TDRBundleFQID]:
        current_bundles = self._query_unique_sorted(f'''
            SELECT links_id, version
            FROM {backtick(self._full_table_name(source.spec, 'links'))}
            WHERE STARTS_WITH(links_id, '{prefix}')
        ''', group_by='links_id')
        return [
            TDRBundleFQID(source=source,
                          uuid=row['links_id'],
                          version=self.format_version(row['version']))
            for row in current_bundles
        ]

    def _query_unique_sorted(self,
                             query: str,
                             group_by: str
                             ) -> list[BigQueryRow]:
        iter_rows = self._run_sql(query)
        key = itemgetter(group_by)
        rows = sorted(iter_rows, key=key)
        require(len(set(map(key, rows))) == len(rows), 'Expected unique keys', group_by)
        return rows

    def _emulate_bundle(self, bundle_fqid: TDRBundleFQID) -> TDRHCABundle:
        bundle = TDRHCABundle(fqid=bundle_fqid,
                              manifest={},
                              metadata={},
                              links={})
        entities, root_entities, links_jsons = self._stitch_bundles(bundle)
        bundle.links = self._merge_links(links_jsons)

        with ThreadPoolExecutor(max_workers=config.num_tdr_workers) as executor:
            futures = {
                entity_type: executor.submit(self._retrieve_entities,
                                             bundle.fqid.source.spec,
                                             entity_type,
                                             entity_ids)
                for entity_type, entity_ids in entities.items()
            }
            for entity_type, future in futures.items():
                e = future.exception()
                if e is None:
                    rows = future.result()
                    pk_column = entity_type + '_id'
                    rows.sort(key=itemgetter(pk_column))
                    for row in rows:
                        entity = EntityReference(entity_id=row[pk_column], entity_type=entity_type)
                        is_stitched = entity not in root_entities
                        bundle.add_entity(entity=entity,
                                          row=row,
                                          is_stitched=is_stitched)
                else:
                    log.error('TDR worker failed to retrieve entities of type %r',
                              entity_type, exc_info=e)
                    raise e
        return bundle

    def _stitch_bundles(self,
                        root_bundle: 'TDRHCABundle'
                        ) -> tuple[EntitiesByType, Entities, list[JSON]]:
        """
        Recursively follow dangling inputs to collect entities from upstream
        bundles, ensuring that no bundle is processed more than once.
        """
        source = root_bundle.fqid.source
        entities: EntitiesByType = defaultdict(set)
        root_entities = None
        unprocessed: set[TDRBundleFQID] = {root_bundle.fqid}
        processed: set[TDRBundleFQID] = set()
        stitched_links: list[JSON] = []
        # Retrieving links in batches eliminates the risk of exceeding
        # BigQuery's maximum query size. Using a batches size 1000 appears to be
        # equally performant as retrieving the links without batching.
        batch_size = 1000
        while unprocessed:
            batch = set(islice(unprocessed, batch_size))
            links = self._retrieve_links(batch)
            processed.update(batch)
            unprocessed -= batch
            stitched_links.extend(links.values())
            all_dangling_inputs: set[EntityReference] = set()
            for links_id, links_json in links.items():
                project = EntityReference(entity_type='project',
                                          entity_id=links_json['project_id'])
                links = Links.from_json(project, links_json['content'])
                linked_entities = links.all_entities()
                dangling_inputs = links.dangling_inputs()
                if links_id == root_bundle.fqid:
                    assert root_entities is None
                    root_entities = linked_entities - dangling_inputs
                for entity in linked_entities:
                    entities[entity.entity_type].add(entity.entity_id)
                if dangling_inputs:
                    log.info('There are %i dangling inputs in bundle %r', len(dangling_inputs), links_id)
                    log.debug('Dangling inputs in bundle %r: %r', links_id, dangling_inputs)
                    all_dangling_inputs.update(dangling_inputs)
                else:
                    log.info('Bundle %r is self-contained', links_id)
            if all_dangling_inputs:
                upstream = self._find_upstream_bundles(source, all_dangling_inputs)
                unprocessed |= upstream - processed

        assert root_entities is not None
        processed.remove(root_bundle.fqid)
        if processed:
            arg = f': {processed!r}' if log.isEnabledFor(logging.DEBUG) else ''
            log.info('Stitched %i bundle(s)%s', len(processed), arg)
        return entities, root_entities, stitched_links

    def _retrieve_links(self,
                        links_ids: set[TDRBundleFQID]
                        ) -> dict[TDRBundleFQID, JSON]:
        """
        Retrieve links entities from BigQuery and parse the `content` column.
        :param links_ids: Which links entities to retrieve.
        """
        source = one({fqid.source.spec for fqid in links_ids})
        links = self._retrieve_entities(source, 'links', links_ids)
        links = {
            # Copy the values so we can reassign `content` below
            fqid: dict(one(links_json
                           for links_json in links
                           if links_json['links_id'] == fqid.uuid))
            for fqid in links_ids
        }
        for links_json in links.values():
            links_json['content'] = json.loads(links_json['content'])
        return links

    def _retrieve_entities(self,
                           source: TDRSourceSpec,
                           entity_type: EntityType,
                           entity_ids: set[EntityID] | set[BundleFQID],
                           ) -> list[BigQueryRow]:
        """
        Efficiently retrieve multiple entities from BigQuery in a single query.

        :param source: Snapshot containing the entity table

        :param entity_type: The type of entity, corresponding to the table name

        :param entity_ids: For links, the fully qualified UUID and version of
                           each `links` entity. For other entities, just the UUIDs.
        """
        pk_column = entity_type + '_id'
        version_column = 'version'
        non_pk_columns = (
            TDRHCABundle.links_columns if entity_type == 'links'
            else TDRHCABundle.data_columns if entity_type.endswith('_file')
            else TDRHCABundle.metadata_columns
        )
        table_name = backtick(self._full_table_name(source, entity_type))
        entity_id_type = one(set(map(type, entity_ids)))

        if entity_type == 'links':
            assert issubclass(entity_id_type, BundleFQID), entity_id_type
            entity_ids = cast(set[BundleFQID], entity_ids)
            where_columns = (pk_column, version_column)
            where_values = (
                (sq(fqid.uuid), f'TIMESTAMP({sq(fqid.version)})')
                for fqid in entity_ids
            )
            expected = {fqid.uuid for fqid in entity_ids}
        else:
            assert issubclass(entity_id_type, str), (entity_type, entity_id_type)
            where_columns = (pk_column,)
            where_values = ((sq(str(entity_id)),) for entity_id in entity_ids)
            expected = entity_ids
        query = f'''
            SELECT {', '.join({pk_column, *non_pk_columns})}
            FROM {table_name}
            WHERE {self._in(where_columns, where_values)}
        '''
        log.debug('Retrieving %i entities of type %r ...', len(entity_ids), entity_type)
        rows = self._query_unique_sorted(query, group_by=pk_column)
        log.debug('Retrieved %i entities of type %r', len(rows), entity_type)
        missing = expected - {row[pk_column] for row in rows}
        require(not missing,
                f'Required entities not found in {table_name}: {missing}')
        return rows

    def _in(self,
            columns: tuple[str, ...],
            values: Iterable[tuple[str, ...]]
            ) -> str:
        """
        >>> plugin = Plugin(sources=set())
        >>> plugin._in(('foo', 'bar'), [('"abc"', '123'), ('"def"', '456')])
        '(foo, bar) IN (("abc", 123), ("def", 456))'
        """

        def join(i):
            return '(' + ', '.join(i) + ')'

        return join(columns) + ' IN ' + join(map(join, values))

    def _find_upstream_bundles(self,
                               source: TDRSourceRef,
                               outputs: Entities) -> set[TDRBundleFQID]:
        """
        Search for bundles containing processes that produce the specified output
        entities.
        """
        output_ids = [output.entity_id for output in outputs]
        output_id = 'JSON_EXTRACT_SCALAR(link_output, "$.output_id")'
        rows = self._run_sql(f'''
            SELECT links_id, version, {output_id} AS output_id
            FROM {backtick(self._full_table_name(source.spec, 'links'))} AS links
                JOIN UNNEST(JSON_EXTRACT_ARRAY(links.content, '$.links')) AS content_links
                    ON JSON_EXTRACT_SCALAR(content_links, '$.link_type') = 'process_link'
                JOIN UNNEST(JSON_EXTRACT_ARRAY(content_links, '$.outputs')) AS link_output
                    ON {output_id} IN UNNEST({output_ids})
        ''')
        bundles = set()
        outputs_found = set()
        for row in rows:
            bundles.add(TDRBundleFQID(source=source,
                                      uuid=row['links_id'],
                                      version=self.format_version(row['version'])))
            outputs_found.add(row['output_id'])
        missing = set(output_ids) - outputs_found
        require(not missing,
                f'Dangling inputs not found in any bundle: {missing}')
        return bundles

    def _merge_links(self, links_jsons: JSONs) -> JSON:
        """
        Merge the links.json documents from multiple stitched bundles into a
        single document.
        """
        root, *stitched = links_jsons
        if stitched:
            source_contents = [row['content'] for row in links_jsons]
            # FIXME: Explicitly verify compatible schema versions for stitched subgraphs
            #        https://github.com/DataBiosphere/azul/issues/3215
            schema_type = 'links'
            schema_version = '3.0.0'
            schema_url = furl(url='https://schema.humancellatlas.org',
                              path=('system', schema_version, schema_type))
            merged_content = {
                'schema_type': schema_type,
                'schema_version': schema_version,
                'describedBy': str(schema_url),
                'links': sum((sc['links'] for sc in source_contents), start=[])
            }
            assert merged_content.keys() == one({
                frozenset(sc.keys()) for sc in source_contents
            }), merged_content
            return merged_content
        else:
            return root['content']
