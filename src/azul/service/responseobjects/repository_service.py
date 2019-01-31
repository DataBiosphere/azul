from azul.service import AbstractService
from azul.service.responseobjects.elastic_request_builder import ElasticTransformDump as EsTd


class RepositoryService(AbstractService):

    def __init__(self, url_func):
        self.es_td = EsTd()
        # FIXME: find a better way to get this func.
        self.file_url = url_func

    def get_data(self, entity_type, pagination, filters=None, item_id=None):
        filters = self.parse_filters(filters)
        # FIXME: single item stuff should be abstracted away
        if item_id is not None:
            if entity_type == 'projects':
                filters['file']['projectId'] = {"is": [item_id]}
            else:
                filters['file']['fileId'] = {"is": [item_id]}

        # FIXME: which of these args are really optional??? I think all are necessary and should
        # be passed that way.
        # FIXME: transform_request may throw BadArgumentException. Catch that here??
        response = self.es_td.transform_request(filters=filters,
                                                pagination=pagination,
                                                post_filter=True,
                                                entity_type=entity_type)

        if entity_type == 'files':
            for hit in response['hits']:
                for file in hit['files']:
                    file['url'] = self.file_url(file['uuid'], version=file['version'], replica='aws')

        # FIXME: single item stuff should be abstracted away
        if item_id is not None:
            return response['hits'][0]

        if entity_type == 'projects':
            # Filter out certain fields if getting *list* of projects
            for hit in response['hits']:
                for project in hit['projects']:
                    project.pop('contributors')
                    project.pop('projectDescription')
                    project.pop('publications')

        return response
