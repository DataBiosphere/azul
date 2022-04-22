from logging import (
    getLogger,
)
from time import (
    sleep,
)
from typing import (
    Any,
    Dict,
    List,
    Optional,
)
from uuid import (
    uuid4,
)

import requests

from azul import (
    config,
)

logger = getLogger(__name__)


class CollectionDataAccess:
    DEFAULT_MAX_BACKOFF_TIME = 60

    def __init__(self, access_token: str):
        self.access_token = access_token

    def get(self, uuid: str, version: str):
        url = self.endpoint_url('collections', uuid)
        query = {
            "version": version,
            "replica": "aws"
        }
        response = self.send_request(uuid, 'get', url, query,
                                     exception_class=RetrievalError)
        collection = response.json()
        return dict(uuid=uuid, version=version, collection=collection)

    def create(self, uuid: str, name: str, description: str, version: str, items: List[Dict[str, str]]):
        url = self.endpoint_url('collections')
        query = {
            "uuid": uuid,
            "version": version,
            "replica": "aws"
        }
        payload = {
            "name": name,
            "description": description,
            "details": {},
            "contents": items
        }
        response = self.send_request(uuid, 'put', url, query, payload,
                                     expected_status_code=201,
                                     exception_class=CreationError)
        collection = response.json()
        return dict(uuid=collection['uuid'], version=collection['version'])

    def append(self, uuid: str, version: str, items: List[Dict[str, str]]):
        url = self.endpoint_url('collections', uuid)
        query = {
            "version": version,
            "replica": "aws"
        }
        payload = {
            "add_contents": [item for item in items]
        }
        response = self.send_request(uuid, 'patch', url, query, payload,
                                     exception_class=UpdateError)
        collection = response.json()
        return dict(uuid=collection['uuid'], version=collection['version'])

    def send_request(self, uuid, method: str, url: str, params,
                     payload: Optional[Any] = None,
                     delay: Optional[int] = None,
                     request_id: Optional[str] = None,
                     expected_status_code: Optional[int] = 200,
                     exception_class=None):
        request_id = request_id or str(uuid4())
        # delay_factor is for automatic retry with exponential backoff.
        if delay is not None:
            if delay > self.DEFAULT_MAX_BACKOFF_TIME:
                logger.warning('Request %s: The request fails to respond within the time limit.', request_id)
                raise ServerTimeoutError(uuid)
            logger.info('Request %s: Retrying in %d s', request_id, delay)
            sleep(delay)
            logger.info('Request %s: Resuming', request_id)
        request_params = dict(params=params,
                              headers=dict(Authorization=self.access_token))
        if payload:
            request_params['json'] = payload
        # FIXME "requests" will be replaced with "DSSClient".
        response = getattr(requests, method)(url, **request_params)
        logger.info('Request %s: %s -> HTTP %s', request_id, method.upper(), response.status_code)
        response_body = response.json()
        if response.status_code == expected_status_code:
            return response
        elif response_body.get('code') == 'timed_out':
            logger.warning('Request %s: The request seems to end with server timeout. (will retry)', request_id)
            return self.send_request(uuid,
                                     method,
                                     url,
                                     params,
                                     payload,
                                     delay * 2 if delay is not None else 1,
                                     request_id)
        elif response.status_code == 502:
            logger.warning('Request %s: The request seems to end with unknown bad gateway. (will retry)', request_id)
            return self.send_request(uuid,
                                     method,
                                     url,
                                     params,
                                     payload,
                                     delay * 2 if delay is not None else 1,
                                     request_id)
        elif response.status_code == 401:
            logger.error('Request %s: Detected authorization error', request_id)
            logger.info('Request %s: Collection %s', request_id, uuid)
            logger.info('Request %s: Remote Error Code: %s', request_id, response.json().get('code'))
            raise UnauthorizedClientAccessError(uuid)
        else:
            error_data = response.json()
            logger.error('Request %s: Expecting HTTP %d (given HTTP %d)',
                         request_id,
                         expected_status_code,
                         response.status_code)
            logger.info('Request %s: Collection %s', request_id, uuid)
            logger.info('Request %s: Remote Error Code: %s', request_id, error_data.get('code'))
            logger.info('Request %s: Remote Traceback: %s', request_id, error_data.get('stacktrace'))
            raise (exception_class or ClientError)(uuid)

    @staticmethod
    def endpoint_url(*request_path):
        return f'{config.dss_endpoint}/{"/".join(request_path)}'


class ClientError(RuntimeError):
    pass


class RetrievalError(ClientError):
    pass


class CreationError(ClientError):
    pass


class UpdateError(ClientError):
    pass


class ServerTimeoutError(ClientError):
    pass


class UnauthorizedClientAccessError(ClientError):
    pass
