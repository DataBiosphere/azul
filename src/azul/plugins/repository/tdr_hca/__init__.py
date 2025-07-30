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
    ClassVar,
    Iterable,
    Self,
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
    R,
    config,
    iif,
)
from azul.bigquery import (
    BigQueryRow,
    backtick,
)
from azul.collections import (
    singleton,
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
from azul.plugins.metadata.hca import (
    HCAFile,
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
    JSON,
    JSONs,
    MutableJSON,
    MutableJSONs,
)
from humancellatlas.data.metadata import (
    api,
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
    def from_json(cls, project: EntityReference, links_json: JSON) -> Self:
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
                assert associate == project, R(
                    'Supplementary file must be associated with the current project',
                    project, associate)
                for entity in cast(JSONs, link['files']):
                    self.supplementary_files.add(
                        EntityReference(entity_type='supplementary_file',
                                        entity_id=entity['file_id']))
            else:
                assert False, R('Unexpected link_type', link_type)
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
            self._add_manifest_entry(entity, self.file_from_row(row))
        content = row['content']
        self.metadata[str(entity)] = (json.loads(content)
                                      if isinstance(content, str)
                                      else content)

    metadata_columns: ClassVar[frozenset[str]] = singleton(
        'content'
    )

    data_columns: ClassVar[frozenset[str]] = frozenset({
        'descriptor',
        'JSON_EXTRACT_SCALAR(content, "$.file_core.file_name") AS file_name',
        'file_id'
    })

    # `links_id` is omitted for consistency since the other sets do not include
    # the primary key
    links_columns: ClassVar[frozenset[str]] = singleton(
        'project_id'
    )

    @classmethod
    def file_from_row(cls, row: BigQueryRow) -> HCAFile:
        descriptor = json.loads(row['descriptor'])
        # FIXME: Move validation of descriptor to the metadata API
        #        https://github.com/DataBiosphere/azul/issues/6299
        api.Entity.validate_described_by(descriptor)
        return HCAFile.from_descriptor(descriptor,
                                       uuid=descriptor['file_id'],
                                       name=row['file_name'],
                                       drs_uri=cls._parse_drs_uri(row['file_id'], descriptor))

    def _add_manifest_entry(self,
                            entity: EntityReference,
                            file: HCAFile) -> None:
        file_json = file.to_json()
        file_json['content-type'] = file_json.pop('content_type')
        file_json['indexed'] = False
        self.manifest[str(entity)] = file_json

    @classmethod
    def _parse_drs_uri(cls,
                       file_id: str | None,
                       descriptor: JSON
                       ) -> str | None:
        if file_id is None:
            try:
                external_drs_uri = descriptor['drs_uri']
            except KeyError:
                assert False, R(
                    '`file_id` is null and `drs_uri` is not set in file descriptor',
                    descriptor)
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
            assert parsed.uri.netloc == config.tdr_service_url.netloc, R(
                'Unexpected DRS URI location', parsed.uri)
            return file_id


class Plugin(TDRPlugin[TDRHCABundle, TDRBundleFQID]):

    def count_bundles(self, source: TDRSourceSpec) -> int:
        prefix = '' if source.prefix is None else source.prefix.common
        assert prefix == prefix.lower(), source
        query = f'''
        SELECT COUNT(*) AS count
        FROM {backtick(self._full_table_name(source, 'links'))}
        WHERE STARTS_WITH(LOWER(datarepo_row_id), {prefix!r})
        '''
        rows = self._run_sql(query)
        return one(rows)['count']

    def count_files(self, source: TDRSourceSpec) -> int:
        prefix = '' if source.prefix is None else source.prefix.common
        assert prefix == prefix.lower(), source
        query = ' UNION ALL '.join(
            f'''
            SELECT COUNT(*) AS count
            FROM {backtick(self._full_table_name(source, entity_type))}
            WHERE STARTS_WITH(LOWER(JSON_EXTRACT_SCALAR(descriptor, "$.sha256")),
                              {prefix!r})
            '''
            for entity_type, entity_cls in api.entity_types.items()
            if entity_type.endswith('_file')
        )
        rows = self._run_sql(query)
        return sum(row['count'] for row in rows)

    def list_bundles(self,
                     source: TDRSourceRef,
                     prefix: str
                     ) -> list[TDRBundleFQID]:
        self._assert_source(source)
        self._assert_partition(source, prefix)
        assert prefix == prefix.lower(), source
        current_bundles = self._query_unique_sorted(f'''
            SELECT links_id, version
            FROM {backtick(self._full_table_name(source.spec, 'links'))}
            WHERE STARTS_WITH(LOWER(links_id), {prefix!r})
        ''', group_by='links_id')
        return [
            TDRBundleFQID(source=source,
                          uuid=row['links_id'],
                          version=self.format_version(row['version']))
            for row in current_bundles
        ]

    def list_files(self, source: TDRSourceRef, prefix: str) -> list[HCAFile]:
        self._assert_source(source)
        self._assert_partition(source, prefix)
        assert prefix == prefix.lower(), prefix
        rows = self._run_sql(' UNION ALL '.join(
            f'''
            SELECT {', '.join(TDRHCABundle.data_columns)}
            FROM {backtick(self._full_table_name(source.spec, entity_type))}
            WHERE STARTS_WITH(LOWER(JSON_EXTRACT_SCALAR(descriptor, "$.sha256")),
                              {prefix!r})
            '''
            for entity_type, entity_cls in api.entity_types.items()
            if entity_type.endswith('_file')
        ))
        return list(map(TDRHCABundle.file_from_row, rows))

    def _query_unique_sorted(self,
                             query: str,
                             group_by: str
                             ) -> list[BigQueryRow]:
        iter_rows = self._run_sql(query)
        key = itemgetter(group_by)
        rows = sorted(iter_rows, key=key)
        assert len(set(map(key, rows))) == len(rows), R(
            'Expected unique keys', group_by)
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
                        root_bundle: TDRHCABundle
                        ) -> tuple[EntitiesByType, Entities, MutableJSONs]:
        """
        Recursively follow dangling inputs to collect entities from upstream
        bundles, ensuring that no bundle is processed more than once.
        """
        source = root_bundle.fqid.source
        entities: EntitiesByType = defaultdict(set)
        root_entities = None
        unprocessed: set[TDRBundleFQID] = {root_bundle.fqid}
        processed: set[TDRBundleFQID] = set()
        stitched_links: MutableJSONs = []
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
                        ) -> dict[TDRBundleFQID, MutableJSON]:
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
        columns = {
            pk_column,
            *TDRHCABundle.metadata_columns,
            *iif(entity_type == 'links', TDRHCABundle.links_columns),
            *iif(entity_type.endswith('_file'), TDRHCABundle.data_columns)
        }
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
            SELECT {', '.join(columns)}
            FROM {table_name}
            WHERE {self._in(where_columns, where_values)}
        '''
        log.debug('Retrieving %i entities of type %r ...', len(entity_ids), entity_type)
        rows = self._query_unique_sorted(query, group_by=pk_column)
        log.debug('Retrieved %i entities of type %r', len(rows), entity_type)
        missing = expected - {row[pk_column] for row in rows}
        assert not missing, R(
            f'Found only {len(rows)} out of {len(entity_ids)} expected rows in {table_name}. '
            f'Missing entities: {missing}')
        return rows

    def _in(self,
            columns: tuple[str, ...],
            values: Iterable[tuple[str, ...]]
            ) -> str:
        """
        >>> plugin = Plugin(catalog='')
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
        assert not missing, R(f'Dangling inputs not found in any bundle: {missing}')
        return bundles

    def _merge_links(self, links_jsons: MutableJSONs) -> MutableJSON:
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
