from concurrent.futures import ThreadPoolExecutor

from more_itertools import one
from typing_extensions import Protocol

from azul import CatalogName
from azul.service import (
    Filters,
    MutableFilters,
)
from azul.service.elasticsearch_service import ElasticsearchService
from azul.uuids import validate_uuid


class FileUrlFunc(Protocol):

    def __call__(self, uuid: str, fetch: bool = True, **params: str) -> str:
        pass


class EntityNotFoundError(Exception):

    def __init__(self, entity_type: str, entity_id: str):
        super().__init__(f"Can't find an entity in {entity_type} with an uuid, {entity_id}.")


class RepositoryService(ElasticsearchService):

    def _get_data(self, catalog: CatalogName, entity_type, pagination, filters: Filters, file_url_func):
        # FIXME: which of these args are really optional? (looks like none of them)
        response = self.transform_request(catalog=catalog,
                                          filters=filters,
                                          pagination=pagination,
                                          post_filter=True,
                                          entity_type=entity_type)
        if entity_type in ('files', 'bundles'):
            # Compose URL to contents of file so clients can download easily
            for hit in response['hits']:
                for file in hit['files']:
                    file['url'] = file_url_func(file['uuid'], version=file['version'], replica='aws')
        return response

    def _get_item(self, catalog: CatalogName, entity_type, item_id, pagination, filters: MutableFilters, file_url_func):
        filters['entryId'] = {'is': [item_id]}
        validate_uuid(item_id)
        response = self._get_data(catalog, entity_type, pagination, filters, file_url_func)
        return one(response['hits'], too_short=EntityNotFoundError(entity_type, item_id))

    def _get_items(self, catalog: CatalogName, entity_type, pagination, filters: Filters, file_url_func):
        response = self._get_data(catalog, entity_type, pagination, filters, file_url_func)
        return response

    def get_data(self,
                 catalog: CatalogName,
                 entity_type,
                 pagination,
                 filters: str,
                 item_id,
                 file_url_func: FileUrlFunc):
        """
        Returns data for a particular entity type of single item.
        :param catalog: The name of the catalog to query
        :param entity_type: Which index to search (i.e. 'projects', 'specimens', etc.)
        :param pagination: A dictionary with pagination information as return from `_get_pagination()`
        :param filters: None, or unparsed string of JSON filters from the request
        :param item_id: If item_id is specified, only a single item is searched for
        :param file_url_func: A function that is used only when getting a *list* of files data.
        It creates the files URL based on info from the request. It should have the type
        signature `(uuid: str, **params) -> str`
        :return: The Elasticsearch JSON response
        """
        filters = self.parse_filters(filters)
        if item_id is None:
            return self._get_items(catalog, entity_type, pagination, filters, file_url_func)
        else:
            return self._get_item(catalog, entity_type, item_id, pagination, filters, file_url_func)

    def get_summary(self, catalog: CatalogName, filters):
        filters = self.parse_filters(filters)
        # Request a summary for each entity type and cherry-pick summary fields from the summaries for the entity
        # that is authoritative for those fields.
        summary_fields_by_authority = {
            'files': [
                'totalFileSize',
                'fileTypeSummaries',
                'fileCount',
            ],
            'samples': [
                'organTypes',
                'donorCount',
                'specimenCount',
                'speciesCount'
            ],
            'projects': [
                'projectCount',
                'labCount',
            ],
            'cell_suspensions': [
                'totalCellCount',
                'cellCountSummaries',
            ]
        }

        def make_summary(entity_type):
            """Returns the key and value for a dict entry to transformation summary"""
            return entity_type, self.transform_summary(catalog=catalog,
                                                       filters=filters,
                                                       entity_type=entity_type)

        with ThreadPoolExecutor(max_workers=len(summary_fields_by_authority)) as executor:
            summaries = dict(executor.map(make_summary,
                                          summary_fields_by_authority))
        unified_summary = {field: summaries[entity_type][field]
                           for entity_type, summary_fields in summary_fields_by_authority.items()
                           for field in summary_fields}
        assert all(len(unified_summary) == len(summary) for summary in summaries.values())
        return unified_summary

    def get_search(self, catalog: CatalogName, entity_type, pagination, filters: str, _query, field):
        filters = self.parse_filters(filters)
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
