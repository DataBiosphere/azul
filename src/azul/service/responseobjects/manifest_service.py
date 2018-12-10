import base64
import binascii
import json
from typing import Tuple

from azul import config
from azul.service.responseobjects.step_function_helper import StateMachineError, StepFunctionHelper


class ManifestService:
    """
    Class containing logic for starting and checking the status of manifest generation jobs
    """
    step_function_helper = StepFunctionHelper()

    def encode_params(self, params: dict) -> str:
        return base64.urlsafe_b64encode(bytes(json.dumps(params), encoding='utf-8')).decode('utf-8')

    def decode_params(self, token: str) -> dict:
        return json.loads(base64.urlsafe_b64decode(token).decode('utf-8'))

    def start_manifest_generation(self, filters: dict, execution_id: str):
        """
        Start the execution of a state machine generating the manifest

        :param filters: filters to use for the manifest
        :param execution_id: name to give the execution (must be unique across executions of the state machine)
        """
        self.step_function_helper.start_execution(config.manifest_state_machine_name,
                                                  execution_id,
                                                  execution_input={'filters': filters})

    def get_manifest_status(self, token: str, retry_url: str) -> Tuple[int, int, str]:
        """
        Get the status of a manifest generation job (in progress, success, or failed)

        :param token: Encoded parameters to use when checking the status of the execution
        :param retry_url: URL to direct the client to if the manifest is still generating
        :return: tuple of HTTP status code, Retry-After value, and URL to request on retry
        """
        try:
            params = self.decode_params(token)
            if 'execution_id' not in params:
                raise KeyError
        except (KeyError, UnicodeDecodeError, binascii.Error, json.decoder.JSONDecodeError):
            raise ValueError('Invalid token given')

        execution = self.step_function_helper.describe_execution(
            config.manifest_state_machine_name, params['execution_id'])

        if execution['status'] == 'SUCCEEDED':
            execution_output = json.loads(execution['output'])
            return 302, 0, execution_output['Location']
        elif execution['status'] == 'RUNNING':
            wait_times = [1, 1, 2, 6, 10]
            wait = max(0, min(params.get('wait', 0), len(wait_times) - 1))
            params['wait'] = wait + 1
            return 301, wait_times[wait], f'{retry_url}?token={self.encode_params(params)}'
        raise StateMachineError('Failed to generate manifest')
