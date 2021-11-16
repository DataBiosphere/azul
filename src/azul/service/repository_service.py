from concurrent.futures import (
    ThreadPoolExecutor,
)
from typing import (
    Optional,
    Set,
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
)
from azul.json import (
    copy_json,
)
from azul.service import (
    FileUrlFunc,
    Filters,
    MutableFilters,
)
from azul.service.elasticsearch_service import (
    ElasticsearchService,
    Pagination,
)
from azul.service.hca_response_v5 import (
    SummaryResponse,
)
from azul.types import (
    AnyMutableJSON,
    JSON,
    MutableJSON,
)
from azul.uuids import (
    validate_uuid,
)


class EntityNotFoundError(Exception):

    def __init__(self, entity_type: str, entity_id: str):
        super().__init__(f"Can't find an entity in {entity_type} with an uuid, {entity_id}.")


class RepositoryService(ElasticsearchService):

    def get_data(self,
                 *,
                 catalog: CatalogName,
                 entity_type: str,
                 file_url_func: FileUrlFunc,
                 source_ids: Set[str],
                 item_id: Optional[str],
                 filters: MutableFilters,
                 pagination: Optional[Pagination]) -> JSON:
        """
        Returns data for a particular entity type of single item.
        :param catalog: The name of the catalog to query
        :param entity_type: Which index to search (i.e. 'projects', 'specimens', etc.)
        :param pagination: A dictionary with pagination information as return from `_get_pagination()`
        :param filters: parsed JSON filters from the request
        :param source_ids: Which sources are accessible
        :param item_id: If item_id is specified, only a single item is searched for
        :param file_url_func: A function that is used only when getting a *list* of files data.
        It creates the files URL based on info from the request. It should have the type
        signature `(uuid: str, **params) -> str`
        :return: The Elasticsearch JSON response
        """
        if item_id is not None:
            validate_uuid(item_id)
            filters['entryId'] = {'is': [item_id]}
        if entity_type != 'projects':
            self._add_implicit_sources_filter(filters, source_ids)
        response = self.transform_request(catalog=catalog,
                                          filters=filters,
                                          pagination=pagination,
                                          entity_type=entity_type)

        for hit in response['hits']:
            entity = one(hit[entity_type])
            source_id = one(hit['sources'])['sourceId']
            entity['accessible'] = source_id in source_ids

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

        inject_file_urls(response['hits'], 'projects', 'contributorMatrices')
        inject_file_urls(response['hits'], 'projects', 'matrices')
        inject_file_urls(response['hits'], 'files')

        if item_id is not None:
            response = one(response['hits'], too_short=EntityNotFoundError(entity_type, item_id))
        return response

    def get_summary(self,
                    catalog: CatalogName,
                    filters: Filters,
                    source_ids: Set[str]
                    ):
        aggs_by_authority = {
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
                'projectEstimatedCellCount'
            ],
            'cell_suspensions': [
                'totalCellCount',
                'cellCountSummaries'
            ]
        }
        source_modified_filters = copy_json(filters)
        self._add_implicit_sources_filter(source_modified_filters, source_ids)

        def transform_summary(entity_type):
            """Returns the key and value for a dict entry to transformation summary"""
            entity_filters = (filters
                              if entity_type == 'projects'
                              else source_modified_filters)
            return entity_type, self.transform_summary(catalog=catalog,
                                                       filters=entity_filters,
                                                       entity_type=entity_type)

        with ThreadPoolExecutor(max_workers=len(aggs_by_authority)) as executor:
            aggs = dict(executor.map(transform_summary, aggs_by_authority))

        aggs = {
            agg_name: aggs[entity_type][agg_name]
            for entity_type, summary_fields in aggs_by_authority.items()
            for agg_name in summary_fields
        }

        response = SummaryResponse(aggs).return_response().to_json()
        for field, nested_field in (
            ('totalFileSize', 'totalSize'),
            ('fileCount', 'count')
        ):
            value = response[field]
            nested_sum = sum(fs[nested_field] for fs in response['fileTypeSummaries'])
            assert value == nested_sum, (value, nested_sum)
        return response

    def get_search(self,
                   catalog: CatalogName,
                   entity_type: str,
                   pagination: Pagination,
                   filters: Filters,
                   _query: str,
                   field: str):
        # HACK: Adding this small check to make sure the search bar works with
        if entity_type in ('donor', 'file-donor'):
            field = 'donor'
        response = self.transform_autocomplete_request(catalog,
                                                       pagination,
                                                       filters=filters,
                                                       _query=_query,
                                                       search_field=field,
                                                       entry_format=entity_type)
        return response

    def get_data_file(self,
                      catalog: CatalogName,
                      file_uuid: str,
                      file_version: Optional[str],
                      filters: Filters,
                      source_ids: Set[str]
                      ) -> Optional[MutableJSON]:
        """
        Return the inner `files` entity describing the data file with the
        given UUID and version.

        :param catalog: the catalog to search in

        :param file_uuid: the UUID of the data file

        :param file_version: the version of the data file, if absent the most
                             recent version will be returned

        :param filters: None, or parsed JSON filters from the request

        :return: The inner `files` entity or None if the catalog does not
                 contain information about the specified data file

        :param source_ids: Which sources are accessible
        """
        filters = {
            'fileId': {'is': [file_uuid]},
            **({} if file_version is None else {'fileVersion': {'is': [file_version]}}),
            **({} if filters is None else filters)
        }
        self._add_implicit_sources_filter(filters, source_ids)

        def _hit_to_doc(hit: Hit) -> JSON:
            return self.translate_fields(catalog, hit.to_dict(), forward=False)

        es_search = self._create_request(catalog=catalog,
                                         filters=filters,
                                         post_filter=False,
                                         enable_aggregation=False,
                                         entity_type='files')
        if file_version is None:
            doc_path = self.service_config(catalog).translation['fileVersion']
            es_search.sort({doc_path: dict(order='desc')})

        # Just need two hits to detect an ambiguous response
        es_search.params(size=2)

        hits = list(map(_hit_to_doc, es_search.execute().hits))

        if len(hits) == 0:
            return None
        elif len(hits) > 1:
            # Can't have more than one hit with the same version
            assert file_version is None, len(hits)

        file = one(first(hits)['contents']['files'])
        if file_version is not None:
            assert file_version == file['version']
        return file
