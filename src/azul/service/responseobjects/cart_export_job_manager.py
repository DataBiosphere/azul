from base64 import urlsafe_b64encode, urlsafe_b64decode
import binascii
from datetime import datetime
from json import dumps as json_dumps, loads as json_loads, decoder
import logging
import uuid

from azul.service.step_function_helper import StepFunctionHelper
from azul import config

logger = logging.getLogger(__name__)


class CartExportJobManager:
    step_function_helper = StepFunctionHelper()

    @staticmethod
    def encode_token(params):
        return urlsafe_b64encode(bytes(json_dumps(params), encoding='utf-8')).decode('utf-8')

    @staticmethod
    def decode_token(token):
        return json_loads(urlsafe_b64decode(token).decode('utf-8'))

    def initiate(self, user_id: str, cart_id: str, access_token: str):
        execution_id = str(uuid.uuid4())
        collection_uuid = str(uuid.uuid4())
        collection_version = datetime.utcnow().strftime('%Y-%m-%dT%H%M%S.000000Z')
        self.step_function_helper.start_execution(config.cart_export_state_machine_name,
                                                  execution_name=execution_id,
                                                  execution_input=dict(execution_id=execution_id,
                                                                       user_id=user_id,
                                                                       cart_id=cart_id,
                                                                       collection_uuid=collection_uuid,
                                                                       collection_version=collection_version,
                                                                       resume_token=None,
                                                                       access_token=access_token))
        return self.encode_token({'execution_id': execution_id})

    def get(self, token):
        try:
            params = self.decode_token(token)
        except (UnicodeDecodeError, binascii.Error, decoder.JSONDecodeError):
            raise InvalidExecutionTokenError('Invalid job token (malform)')
        try:
            execution_id = params['execution_id']
        except KeyError:
            raise InvalidExecutionTokenError('Invalid job token (missing data)')

        event_type_to_detail_field_map = {
            'LambdaFunctionScheduled': 'lambdaFunctionScheduledEventDetails',
            'TaskStateEntered': 'stateEnteredEventDetails',
            'ChoiceStateExited': 'stateExitedEventDetails',
            'ChoiceStateEntered': 'stateEnteredEventDetails',
            'TaskStateExited': 'stateExitedEventDetails',
            'LambdaFunctionSucceeded': 'lambdaFunctionSucceededEventDetails',
            'SucceedStateEntered': 'stateEnteredEventDetails',
            'SucceedStateExited': 'stateExitedEventDetails'
        }

        execution = self.step_function_helper.describe_execution(config.cart_export_state_machine_name, execution_id)
        events = self.step_function_helper.get_execution_history(config.cart_export_state_machine_name, execution_id)

        current_state = None
        last_updated_at = None
        error = None
        for event in events:
            if not last_updated_at:
                last_updated_at = event['timestamp']
            event_type = event['type']
            if current_state is None and event_type in event_type_to_detail_field_map:
                event_detail_field_name = event_type_to_detail_field_map[event_type]
                event_details = event[event_detail_field_name]
                if 'Entered' in event_type or event_type.endswith('Scheduled'):
                    current_state = json_loads(event_details['input'])
                else:
                    current_state = json_loads(event_details['output'])
            if event_type == 'ExecutionFailed':
                event_details = event['executionFailedEventDetails']
                error = dict(
                    code=event_details['error'],
                    cause=event_details['cause']
                )
                logger.error('ERROR: %s: %s', event_details['error'], event_details['cause'])

        return {
            'status': execution['status'],
            'user_id': json_loads(execution['input'])['user_id'],
            'started_at': execution['startDate'],
            'stopped_at': execution.get('stopDate'),
            'final': execution['status'] != 'RUNNING',
            'last_update': {
                'when': last_updated_at,
                'state': current_state,
                'error': error
            }
        }


class InvalidExecutionTokenError(ValueError):
    pass
