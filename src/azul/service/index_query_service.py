from concurrent.futures import (
    ThreadPoolExecutor,
)
from typing import (
    Optional,
    Union,
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
from azul.service import (
    FileUrlFunc,
)
from azul.service.elasticsearch_service import (
    ElasticsearchService,
    Pagination,
)
from azul.types import (
    JSON,
    JSONs,
    MutableJSON,
    MutableJSONs,
)
from azul.uuids import (
    validate_uuid,
)


class EntityNotFoundError(Exception):

    def __init__(self, entity_type: str, entity_id: str):
        super().__init__(f"Can't find an entity in {entity_type} with an uuid, {entity_id}.")


class IndexQueryService(ElasticsearchService):

    def get_data(self,
                 catalog: CatalogName,
                 entity_type: str,
                 file_url_func: FileUrlFunc,
                 item_id: Optional[str] = None,
                 filters: Optional[str] = None,
                 pagination: Optional[Pagination] = None) -> JSON:
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
        if item_id is not None:
            validate_uuid(item_id)
            filters['entryId'] = {'is': [item_id]}
        response = self.transform_request(catalog=catalog,
                                          filters=filters,
                                          pagination=pagination,
                                          entity_type=entity_type)
        # FIXME: Generalize file URL injection.
        #        https://github.com/DataBiosphere/azul/issues/2545
        if entity_type in ('files', 'bundles'):
            # Inject URLs to data files
            for hit in response['hits']:
                for file in hit['files']:
                    file['url'] = file_url_func(catalog=catalog,
                                                file_uuid=file['uuid'],
                                                version=file['version'])
        elif entity_type == 'projects':
            # Similarly, inject URLs to matrix files in stratification trees
            def transform(node: Union[JSONs, JSON]) -> Union[MutableJSONs, MutableJSON]:
                if isinstance(node, dict):
                    return {k: transform(v) for k, v in node.items()}
                elif isinstance(node, list):
                    return [
                        {
                            'name': file['name'],
                            'url': file_url_func(catalog=catalog,
                                                 file_uuid=file['uuid'],
                                                 version=file['version'])
                        }
                        for file in node
                    ]
                else:
                    assert False

            for hit in response['hits']:
                for project in hit['projects']:
                    key = 'contributorMatrices'
                    project[key] = transform(project[key])

        if item_id is not None:
            response = one(response['hits'], too_short=EntityNotFoundError(entity_type, item_id))
        return response

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

    def get_data_file(self,
                      catalog: CatalogName,
                      file_uuid: str,
                      file_version: Optional[str]) -> Optional[MutableJSON]:
        """
        Return the inner `files` entity describing the data file with the
        given UUID and version.

        :param catalog: the catalog to search in

        :param file_uuid: the UUID of the data file

        :param file_version: the version of the data file, if absent the most
                             recent version will be returned

        :return: The inner `files` entity or None if the catalog does not
                 contain information about the specified data file
        """
        filters = {
            'fileId': {'is': [file_uuid]},
            **({} if file_version is None else {'fileVersion': {'is': [file_version]}})
        }

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
