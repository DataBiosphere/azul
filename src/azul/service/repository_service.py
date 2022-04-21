from concurrent.futures import (
    ThreadPoolExecutor,
)
import json
import logging
from typing import (
    Mapping,
    Optional,
    Sequence,
)

import elasticsearch
from elasticsearch_dsl import (
    Q,
    Search,
)
from elasticsearch_dsl.response import (
    Hit,
)
from more_itertools import (
    first,
    one,
)

from azul import (
    CatalogName,
    cached_property,
    config,
)
from azul.service import (
    BadArgumentException,
    FileUrlFunc,
    Filters,
)
from azul.service.elasticsearch_service import (
    AggregationStage,
    ElasticsearchService,
    ElasticsearchStage,
    IndexNotFoundError,
    Pagination,
    PaginationStage,
    ResponseTriple,
    ToDictStage,
    _ElasticsearchStage,
)
from azul.service.hca_response_v5 import (
    SearchResponse,
    SearchResponseFactory,
    SummaryResponse,
    SummaryResponseFactory,
)
from azul.types import (
    AnyMutableJSON,
    JSON,
    MutableJSON,
)
from azul.uuids import (
    validate_uuid,
)

log = logging.getLogger(__name__)


class EntityNotFoundError(Exception):

    def __init__(self, entity_type: str, entity_id: str):
        super().__init__(f"Can't find an entity in {entity_type} with an uuid, {entity_id}.")


class SummaryAggregationStage(AggregationStage):

    def prepare_request(self, request: Search) -> Search:
        request = super().prepare_request(request)
        entity_type = self.entity_type

        def add_filters_sum_agg(parent_field, parent_bucket, child_field, child_bucket):
            parent_field_type = self.service.field_type(self.catalog, tuple(parent_field.split('.')))
            null_value = parent_field_type.to_index(None)
            request.aggs.bucket(
                parent_bucket,
                'filters',
                filters={
                    'hasSome': Q('bool', must=[
                        Q('exists', field=parent_field),  # field exists...
                        Q('bool', must_not=[  # ...and is not zero or null
                            Q('terms', **{parent_field: [0, null_value]})
                        ])
                    ])
                },
                other_bucket_key='hasNone',
            ).metric(
                child_bucket,
                'sum',
                field=child_field
            )

        if entity_type == 'files':
            # Add a total file size aggregate
            request.aggs.metric('totalFileSize',
                                'sum',
                                field='contents.files.size_')
        elif entity_type == 'cell_suspensions':
            # Add a cell count aggregate per organ
            request.aggs.bucket(
                'cellCountSummaries',
                'terms',
                field='contents.cell_suspensions.organ.keyword',
                size=config.terms_aggregation_size
            ).bucket(
                'cellCount',
                'sum',
                field='contents.cell_suspensions.total_estimated_cells_'
            )
        elif entity_type == 'samples':
            # Add an organ aggregate to the Elasticsearch request
            request.aggs.bucket('organTypes',
                                'terms',
                                field='contents.samples.effective_organ.keyword',
                                size=config.terms_aggregation_size)
        elif entity_type == 'projects':
            # Add project cell count sum aggregates from the projects with and
            # without any cell suspension cell counts.
            add_filters_sum_agg(parent_field='contents.cell_suspensions.total_estimated_cells',
                                parent_bucket='cellSuspensionCellCount',
                                child_field='contents.projects.estimated_cell_count_',
                                child_bucket='projectCellCount')
            # Add cell suspensions cell count sum aggregates from projects
            # with and without a project level estimated cell count.
            add_filters_sum_agg(parent_field='contents.projects.estimated_cell_count',
                                parent_bucket='projectCellCount',
                                child_field='contents.cell_suspensions.total_estimated_cells_',
                                child_bucket='cellSuspensionCellCount')
        else:
            assert False, entity_type

        threshold = config.precision_threshold
        for agg_name, cardinality in self._cardinality_aggregations.items():
            request.aggs.metric(agg_name,
                                'cardinality',
                                field=cardinality + '.keyword',
                                precision_threshold=str(threshold))

        self._annotate_aggs_for_translation(request)
        request = request.extra(size=0)
        return request

    @cached_property
    def _cardinality_aggregations(self) -> Mapping[str, str]:
        return {
            'samples': {
                'specimenCount': 'contents.specimens.document_id',
                'speciesCount': 'contents.donors.genus_species',
                'donorCount': 'contents.donors.document_id',
            },
            'projects': {
                'labCount': 'contents.projects.laboratory',
            }
        }.get(self.entity_type, {})

    def process_response(self, response: MutableJSON) -> MutableJSON:
        response = super().process_response(response)
        result = response['aggregations']
        threshold = config.precision_threshold

        for agg_name in self._cardinality_aggregations:
            agg_value = result[agg_name]['value']
            assert agg_value <= threshold / 2, (agg_name, agg_value, threshold)

        return result


class SearchResponseStage(_ElasticsearchStage[ResponseTriple, SearchResponse]):

    def prepare_request(self, request: Search) -> Search:
        return request

    def process_response(self, response: ResponseTriple) -> SearchResponse:
        hits, pagination, aggs = response
        factory = SearchResponseFactory(hits=hits,
                                        pagination=pagination,
                                        aggs=aggs,
                                        entity_type=self.entity_type,
                                        catalog=self.catalog)
        return factory.make_response()


class SummaryResponseStage(ElasticsearchStage[MutableJSON, JSON]):

    @property
    def aggs_by_authority(self) -> Mapping[str, Sequence[str]]:
        return {
            'files': [
                'totalFileSize',
                'fileFormat',
            ],
            'samples': [
                'organTypes',
                'donorCount',
                'specimenCount',
                'speciesCount'
            ],
            'projects': [
                'project',
                'labCount',
                'cellSuspensionCellCount',
                'projectCellCount',
            ],
            'cell_suspensions': [
                'cellCountSummaries',
            ]
        }

    def prepare_request(self, request: Search) -> Search:
        return request

    def process_response(self, response: JSON) -> SummaryResponse:
        factory = SummaryResponseFactory(response)
        response = factory.make_response()
        for field, nested_field in (
            ('totalFileSize', 'totalSize'),
            ('fileCount', 'count')
        ):
            value = response[field]
            nested_sum = sum(fs[nested_field] for fs in response['fileTypeSummaries'])
            assert value == nested_sum, (value, nested_sum)
        return response


class RepositoryService(ElasticsearchService):

    def search(self,
               *,
               catalog: CatalogName,
               entity_type: str,
               file_url_func: FileUrlFunc,
               item_id: Optional[str],
               filters: Filters,
               pagination: Pagination
               ) -> SearchResponse:
        """
        Returns data for a particular entity type of single item.
        :param catalog: The name of the catalog to query
        :param entity_type: Which index to search (i.e. 'projects', 'specimens', etc.)
        :param pagination: A dictionary with pagination information as return from `_get_pagination()`
        :param filters: parsed JSON filters from the request
        :param item_id: If item_id is specified, only a single item is searched for
        :param file_url_func: A function that is used only when getting a *list* of files data.
        It creates the files URL based on info from the request. It should have the type
        signature `(uuid: str, **params) -> str`
        :return: The Elasticsearch JSON response
        """
        if item_id is not None:
            validate_uuid(item_id)
            filters = filters.update({'entryId': {'is': [item_id]}})

        response = self._search(catalog=catalog,
                                filters=filters,
                                pagination=pagination,
                                entity_type=entity_type)

        for hit in response['hits']:
            entity = one(hit[entity_type])
            source_id = one(hit['sources'])['sourceId']
            entity['accessible'] = source_id in filters.source_ids

        def inject_file_urls(node: AnyMutableJSON, *path: str) -> None:
            if node is None:
                pass
            elif isinstance(node, (str, int, float, bool)):
                pass
            elif isinstance(node, list):
                for child in node:
                    inject_file_urls(child, *path)
            elif isinstance(node, dict):
                if path:
                    try:
                        next_node = node[path[0]]
                    except KeyError:
                        # Not all node trees will match the given path. (e.g. a
                        # response from the 'files' index won't have a
                        # 'matrices' in its 'hits[].projects' inner entities.
                        pass
                    else:
                        inject_file_urls(next_node, *path[1:])
                else:
                    try:
                        url = node['url']
                        version = node['version']
                        uuid = node['uuid']
                    except KeyError:
                        for child in node.values():
                            inject_file_urls(child, *path)
                    else:
                        if url is None:
                            node['url'] = file_url_func(catalog=catalog,
                                                        fetch=False,
                                                        file_uuid=uuid,
                                                        version=version)
            else:
                assert False

        inject_file_urls(response['hits'], 'projects', 'contributedAnalyses')
        inject_file_urls(response['hits'], 'projects', 'matrices')
        inject_file_urls(response['hits'], 'files')

        if item_id is not None:
            response = one(response['hits'], too_short=EntityNotFoundError(entity_type, item_id))
        return response

    def _search(self,
                *,
                catalog: CatalogName,
                entity_type: str,
                filters: Filters,
                pagination: Pagination
                ) -> MutableJSON:
        """
        This function does the whole transformation process. It takes the path
        of the config file, the filters, and pagination, if any. Excluding
        filters will do a match_all request. Excluding pagination will exclude
        pagination from the output.

        :param catalog: The name of the catalog to query

        :param entity_type: the string referring to the entity type used to get
                            the ElasticSearch index to search

        :param filters: Filter parameter from the API to be used in the query.

        :param pagination: Pagination to be used for the API

        :return: Returns the transformed request
        """
        plugin = self.metadata_plugin(catalog)
        field_mapping = plugin.field_mapping

        for facet in filters.explicit.keys():
            if facet not in field_mapping:
                raise BadArgumentException(f"Unable to filter by undefined facet {facet}.")

        facet = pagination.sort
        if facet not in field_mapping:
            raise BadArgumentException(f"Unable to sort by undefined facet {facet}.")

        chain = self.create_chain(catalog=catalog,
                                  entity_type=entity_type,
                                  filters=filters,
                                  post_filter=True,
                                  document_slice=None)

        chain = ToDictStage(service=self,
                            catalog=catalog,
                            entity_type=entity_type).wrap(chain)

        chain = AggregationStage.create_and_wrap(chain)

        chain = PaginationStage(service=self,
                                catalog=catalog,
                                entity_type=entity_type,
                                pagination=pagination,
                                peek_ahead=True,
                                filters=filters).wrap(chain)

        chain = SearchResponseStage(service=self,
                                    catalog=catalog,
                                    entity_type=entity_type).wrap(chain)

        request = self.create_request(catalog, entity_type)
        request = chain.prepare_request(request)
        try:
            response = request.execute(ignore_cache=True)
        except elasticsearch.NotFoundError as e:
            raise IndexNotFoundError(e.info["error"]["index"])
        response = chain.process_response(response)
        return response

    def summary(self,
                catalog: CatalogName,
                filters: Filters
                ) -> SummaryResponse:
        # FIXME: Due to the fact that we run multiple requests in parallel each
        #        in a separate chain, and the resulting need to multiplex the
        #        responses, the response stage is not part of any chain.
        #        https://github.com/DataBiosphere/azul/issues/4128
        response_stage = SummaryResponseStage()

        aggs_by_authority = response_stage.aggs_by_authority

        def summary(entity_type):
            return entity_type, self._summary(catalog=catalog,
                                              entity_type=entity_type,
                                              filters=filters)

        with ThreadPoolExecutor(max_workers=len(aggs_by_authority)) as executor:
            aggs = dict(executor.map(summary, aggs_by_authority))

        aggs = {
            agg_name: aggs[entity_type][agg_name]
            for entity_type, summary_fields in aggs_by_authority.items()
            for agg_name in summary_fields
        }

        response = response_stage.process_response(aggs)
        return response

    def _summary(self,
                 *,
                 catalog: CatalogName,
                 entity_type: str,
                 filters: Filters
                 ) -> MutableJSON:
        chain = self.create_chain(catalog=catalog,
                                  entity_type=entity_type,
                                  filters=filters,
                                  post_filter=False,
                                  document_slice=None)
        chain = ToDictStage(service=self,
                            catalog=catalog,
                            entity_type=entity_type).wrap(chain)
        chain = SummaryAggregationStage.create_and_wrap(chain)
        request = chain.prepare_request(self.create_request(catalog, entity_type))

        response = request.execute(ignore_cache=True)
        assert len(response.hits) == 0

        if config.debug == 2 and log.isEnabledFor(logging.DEBUG):
            log.debug('Elasticsearch request: %s', json.dumps(request.to_dict(), indent=4))

        result = chain.process_response(response)

        return result

    def get_data_file(self,
                      catalog: CatalogName,
                      file_uuid: str,
                      file_version: Optional[str],
                      filters: Filters,
                      ) -> Optional[MutableJSON]:
        """
        Return the inner `files` entity describing the data file with the
        given UUID and version.

        :param catalog: the catalog to search in

        :param file_uuid: the UUID of the data file

        :param file_version: the version of the data file, if absent the most
                             recent version will be returned

        :param filters: parsed filters from the request

        :return: The inner `files` entity or None if the catalog does not
                 contain information about the specified data file
        """
        filters = filters.update({
            'fileId': {'is': [file_uuid]},
            **(
                {'fileVersion': {'is': [file_version]}}
                if file_version is not None else
                {}
            )
        })

        def _hit_to_doc(hit: Hit) -> JSON:
            return self.translate_fields(catalog, hit.to_dict(), forward=False)

        entity_type = 'files'
        chain = self.create_chain(catalog=catalog,
                                  entity_type=entity_type,
                                  filters=filters,
                                  post_filter=False,
                                  document_slice=None)
        request = self.create_request(catalog, entity_type)
        request = chain.prepare_request(request)

        if file_version is None:
            plugin = self.metadata_plugin(catalog)
            field_path = plugin.field_mapping['fileVersion']
            request.sort({field_path: dict(order='desc')})

        # Just need two hits to detect an ambiguous response
        request.params(size=2)

        hits = list(map(_hit_to_doc, request.execute().hits))

        if len(hits) == 0:
            return None
        elif len(hits) > 1:
            # Can't have more than one hit with the same version
            assert file_version is None, len(hits)

        file = one(first(hits)['contents']['files'])
        if file_version is not None:
            assert file_version == file['version']
        return file
