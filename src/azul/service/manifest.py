import base64
import binascii
import json
import logging
import uuid

from botocore.exceptions import ClientError
from typing import (
    Tuple,
    Optional,
    Union,
)

from azul import config
from azul.service import AbstractService
from azul.service.step_function_helper import (
    StateMachineError,
    StepFunctionHelper,
)
from azul.types import JSON

logger = logging.getLogger(__name__)


class ManifestService(AbstractService):
    """
    Class containing logic for starting and checking the status of manifest generation jobs
    """
    step_function_helper = StepFunctionHelper()

    @classmethod
    def encode_token(cls, params: dict) -> str:
        return base64.urlsafe_b64encode(bytes(json.dumps(params), encoding='utf-8')).decode('utf-8')

    @classmethod
    def decode_token(cls, token: str) -> dict:
        try:
            token = json.loads(base64.urlsafe_b64decode(token).decode('utf-8'))
            if 'execution_id' not in token:
                raise KeyError
        except (KeyError, UnicodeDecodeError, binascii.Error, json.decoder.JSONDecodeError):
            raise ValueError('Invalid token given')
        else:
            return token

    def start_or_inspect_manifest_generation(self,
                                             self_url,
                                             token: Optional[str] = None,
                                             format: Optional[str] = None,
                                             filters: Optional[str] = None
                                             ) -> Tuple[int, str]:
        """
        If token is None, start a manifest generation process and returns its status.
        Otherwise return the status of the manifest generation process represented by the token.

        :raises ValueError: Will raise a ValueError if token is misformatted or invalid
        :raises StateMachineError: If the state machine fails for some reason.
        :return: Tuple of time to wait and the URL to try. 0 wait time indicates success
        """
        filters = self.parse_filters(filters)

        if token is None:
            execution_id = str(uuid.uuid4())
            self._start_manifest_generation(format, filters, execution_id)
            token = {'execution_id': execution_id}
        else:
            token = self.decode_token(token)

        request_index = token.get('request_index', 0)
        time_or_location = self._get_manifest_status(token['execution_id'], request_index)
        if isinstance(time_or_location, int):
            request_index += 1
            token['request_index'] = request_index
            return time_or_location, f'{self_url}?token={self.encode_token(token)}'
        else:
            return 0, time_or_location

    def _start_manifest_generation(self, format: str, filters: JSON, execution_id: str) -> None:
        """
        Start the execution of a state machine generating the manifest

        :param filters: filters to use for the manifest
        :param execution_id: name to give the execution (must be unique across executions of the state machine)
        """
        self.step_function_helper.start_execution(config.manifest_state_machine_name,
                                                  execution_id,
                                                  execution_input=dict(format=format,
                                                                       filters=filters))

    def _get_next_wait_time(self, request_index: int) -> int:
        wait_times = [1, 1, 4, 6, 10]
        return wait_times[min(request_index, len(wait_times) - 1)]

    def _get_manifest_status(self, execution_id: str, request_index: int) -> Union[int, str]:
        """Returns either the time to wait or the location of the result"""
        try:
            execution = self.step_function_helper.describe_execution(
                config.manifest_state_machine_name, execution_id)
        except ClientError as e:
            if e.response['Error']['Code'] == 'ExecutionDoesNotExist':
                raise ValueError('Invalid token given')
            raise

        if execution['status'] == 'SUCCEEDED':
            # Because describe_execution is eventually consistent output may not yet be present
            if 'output' in execution:
                execution_output = json.loads(execution['output'])
                return execution_output['Location']
            else:
                return 1
        elif execution['status'] == 'RUNNING':
            return self._get_next_wait_time(request_index)
        raise StateMachineError('Failed to generate manifest')
