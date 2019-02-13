import base64
import binascii
import json
import logging
import uuid

from botocore.exceptions import ClientError
from typing import Tuple, Optional, Union

from azul import config
from azul.service import AbstractService
from azul.service.step_function_helper import StateMachineError, StepFunctionHelper

logger = logging.getLogger(__name__)


class ManifestService(AbstractService):
    """
    Class containing logic for starting and checking the status of manifest generation jobs
    """
    step_function_helper = StepFunctionHelper()

    def encode_token(self, params: dict) -> str:
        return base64.urlsafe_b64encode(bytes(json.dumps(params), encoding='utf-8')).decode('utf-8')

    def decode_token(self, token: str) -> dict:
        return json.loads(base64.urlsafe_b64decode(token).decode('utf-8'))

    def start_or_inspect_manifest_generation(self,
                                             self_url,
                                             token: Optional[str] = None,
                                             filters: Optional[str] = None
                                             ) -> Tuple[float, str]:
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
            self._start_manifest_generation(filters, execution_id)
            token = {'execution_id': execution_id}
        else:
            try:
                token = self.decode_token(token)
                if 'execution_id' not in token:  # TODO: Hannes move this validation code into decode_token
                    raise KeyError
            except (KeyError, UnicodeDecodeError, binascii.Error, json.decoder.JSONDecodeError):
                raise ValueError('Invalid token given')

        request_index = token.get('request_index', 0)
        result = self._get_manifest_status(token['execution_id'], request_index)
        if isinstance(result, float):
            request_index += 1
            token['request_index'] = request_index
            return result, f'{self_url}?token={self.encode_token(token)}'
        else:
            return 0, result

    def _start_manifest_generation(self, filters: dict, execution_id: str):
        """
        Start the execution of a state machine generating the manifest

        :param filters: filters to use for the manifest
        :param execution_id: name to give the execution (must be unique across executions of the state machine)
        """
        self.step_function_helper.start_execution(config.manifest_state_machine_name,
                                                  execution_id,
                                                  execution_input={'filters': filters})

    def _get_next_wait_time(self, request_index: int) -> float:
        wait_times = [1, 1, 4, 6, 10]
        return wait_times[min(request_index, len(wait_times) - 1)]

    def _get_manifest_status(self, execution_id: str, request_index: int) -> Union[float, str]:
        """
        Get the status of a manifest generation job (in progress, success, or failed)

        :param token: Encoded parameters to use when checking the status of the execution
        :param retry_url: URL to direct the client to if the manifest is still generating
        :return: Either the time to wait or the location of the result
        """
        try:
            execution = self.step_function_helper.describe_execution(
                config.manifest_state_machine_name, execution_id)
        except ClientError as e:
            if e.response['Error']['Code'] == 'ExecutionDoesNotExist':
                raise ValueError('Invalid token given')
            raise

        if execution['status'] == 'SUCCEEDED':
            execution_output = json.loads(execution['output'])
            return execution_output['Location']
        elif execution['status'] == 'RUNNING':
            return self._get_next_wait_time(request_index)
        raise StateMachineError('Failed to generate manifest')
