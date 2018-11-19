import base64
import json

from azul import config
from azul.service.responseobjects.step_function_helper import StateMachineError, StepFunctionHelper


class ManifestService:
    """
    Class containing logic for starting and checking the status of manifest generation jobs
    """
    manifest_endpoint = '/repository/manifest/files'

    step_function_helper = StepFunctionHelper()

    def encode_params(self, params, encoding='utf-8'):
        return base64.urlsafe_b64encode(bytes(json.dumps(params), encoding=encoding)).decode(encoding)

    def decode_params(self, token, encoding='utf-8'):
        return json.loads(base64.urlsafe_b64decode(token).decode(encoding))

    def start_manifest_generation(self, filters):
        """
        Start the execution of a state machine generating the manifest

        :param filters: filters to use for the manifest
        :return: The id of the execution of the state machine
        """
        return self.step_function_helper.start_execution(
            config.manifest_state_machine_name,
            execution_input={'filters': filters}
        )['executionArn'].split(':')[-1]

    def get_manifest_status(self, params, wait, local=False):
        """
        Get the status of a manifest generation job (in progress, success, or failed)

        :param params: parameters to use when checking the status of the execution
        :param wait: wait step to use as a param in the retry url
        :param local: True if running chalice locally, False otherwise
        :return: dict containing status of the job and a redirect url for the client
        """
        execution = self.step_function_helper.describe_execution(
            config.manifest_state_machine_name, params['execution_id'])

        if execution['status'] == 'SUCCEEDED':
            execution_output = json.loads(execution['output'])
            return {
                'Status': 302,
                'Location': execution_output['Location']
            }
        elif execution['status'] == 'RUNNING':
            wait_times = [1, 1, 2, 6, 10]
            base_url = 'http://localhost:8000' if local else config.service_endpoint()
            return {
                'Status': 301,
                'Retry-After': wait_times[max(0, min(wait, len(wait_times) - 1))],
                'Location': f'{base_url}{self.manifest_endpoint}?token={self.encode_params(params)}&wait={wait}'
            }
        raise StateMachineError('Failed to generate manifest')
