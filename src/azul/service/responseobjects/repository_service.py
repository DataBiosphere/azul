from concurrent.futures import ThreadPoolExecutor

from azul.service import AbstractService
from azul.service.responseobjects.elastic_request_builder import ElasticTransformDump as EsTd


class RepositoryService(AbstractService):

    def __init__(self, url_func):
        self.es_td = EsTd()
        # FIXME: find a better way to get this func.
        self.file_url = url_func

    def _get_data(self, entity_type, pagination, filters=None):
        # FIXME: which of these args are really optional??? (looks like none of them)
        response = self.es_td.transform_request(filters=filters,
                                                pagination=pagination,
                                                post_filter=True,
                                                entity_type=entity_type)
        if entity_type == 'files':
            for hit in response['hits']:
                for file in hit['files']:
                    file['url'] = self.file_url(file['uuid'], version=file['version'], replica='aws')
        return response

    def _get_item(self, entity_type, item_id, pagination, filters):
        if entity_type == 'projects':
            filters['file']['projectId'] = {"is": [item_id]}
        else:
            filters['file']['fileId'] = {"is": [item_id]}
        response = self._get_data(entity_type, pagination, filters)
        return response['hits'][0]

    def _get_items(self, entity_type, pagination, filters):
        response = self._get_data(entity_type, pagination, filters)
        if entity_type == 'projects':
            # Filter out certain fields if getting *list* of projects
            for hit in response['hits']:
                for project in hit['projects']:
                    project.pop('contributors')
                    project.pop('projectDescription')
                    project.pop('publications')
        return response

    def get_data(self, entity_type, pagination, filters=None, item_id=None):
        filters = self.parse_filters(filters)
        if item_id is not None:
            return self._get_item(entity_type, item_id, pagination, filters)
        return self._get_items(entity_type, pagination, filters)

    def get_summary(self, filters):
        filters = self.parse_filters(filters)
        # Request a summary for each entity type and cherry-pick summary fields from the summaries for the entity
        # that is authoritative for those fields.
        summary_fields_by_authority = {
            'files':
                ['totalFileSize', 'fileTypeSummaries', 'fileCount'],
            'specimens':
                ['organCount', 'donorCount', 'labCount', 'totalCellCount', 'organSummaries', 'specimenCount'],
            'projects':
                ['projectCount']
        }

        def make_summary(entity_type):
            """Returns the key and value for a dict entry to transformation summary"""
            return entity_type, self.es_td.transform_summary(filters=filters, entity_type=entity_type)
        with ThreadPoolExecutor(max_workers=len(summary_fields_by_authority)) as executor:
            summaries = dict(executor.map(make_summary,
                                          summary_fields_by_authority))
        unified_summary = {field: summaries[entity_type][field]
                           for entity_type, summary_fields in summary_fields_by_authority.items()
                           for field in summary_fields}
        assert all(len(unified_summary) == len(summary) for summary in summaries.values())
        return unified_summary
