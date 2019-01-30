import ast

from azul.service.responseobjects.elastic_request_builder import (BadArgumentException,
                                                                  ElasticTransformDump as EsTd,
                                                                  IndexNotFoundError)
from azul.service.responseobjects.utilities import json_pp


# def get_specimen():
#     if app.current_request.query_params is None:
#         app.current_request.query_params = {}
#     filters = app.current_request.query_params.get('filters', '{"file": {}}')
#     try:
#         filters = ast.literal_eval(filters)
#         pagination = _get_pagination(app.current_request)
#         # Handle <file_id> request form (for single item)
#         if project_id is not None:
#             filters['file']['projectId'] = {"is": [project_id]}
#         es_td = EsTd()
#         response = es_td.transform_request(filters=filters,
#                                            pagination=pagination,
#                                            post_filter=True,
#                                            entity_type='projects')
#     except BadArgumentException as bae:
#         raise BadRequestError(msg=bae.message)
#     else:
#         # Return a single response if <project_id> request form is used
#         if project_id is not None:
#             return response['hits'][0]
#
#         # Filter out certain fields if getting list of projects
#         for hit in response['hits']:
#             for project in hit['projects']:
#                 project.pop('contributors')
#                 project.pop('projectDescription')
#                 project.pop('publications')
#         return response


class RepositoryService:

    def __init__(self, url_func):
        self.es_td = EsTd()
        # FIXME: find a better way to get this func.
        self.file_url = url_func

    def get_data(self, entity_type, pagination, filters=None, item_id=None):
        if item_id is not None:
            filters['file']['fileId'] = {"is": [item_id]}

        # FIXME: which of these args are really optional???
        response = self.es_td.transform_request(filters=filters,
                                                pagination=pagination,
                                                post_filter=True,
                                                entity_type=entity_type)

        if entity_type == 'files':
            for hit in response['hits']:
                for file in hit['files']:
                    file['url'] = self.file_url(file['uuid'], version=file['version'], replica='aws')

        if item_id is not None:
            response = response['hits'][0]
        return response
