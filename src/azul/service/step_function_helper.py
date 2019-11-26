import json

from azul.deployment import aws


class StepFunctionHelper:
    """
    Wrapper around boto3 SFN client to handle resource name generation and state machine executions
    """

    def state_machine_arn(self, state_machine_name):
        return f'arn:aws:states:{aws.region_name}:{aws.account}:stateMachine:{state_machine_name}'

    def execution_arn(self, state_machine_name, execution_name):
        return f'arn:aws:states:{aws.region_name}:{aws.account}:execution:{state_machine_name}:{execution_name}'

    def start_execution(self, state_machine_name, execution_name, execution_input):
        execution_params = {
            'stateMachineArn': self.state_machine_arn(state_machine_name),
            'name': execution_name,
            'input': json.dumps(execution_input)
        }
        execution_response = aws.stepfunctions.start_execution(**execution_params)
        assert self.execution_arn(state_machine_name, execution_name) == execution_response['executionArn']

    def describe_execution(self, state_machine_name, execution_name):
        return aws.stepfunctions.describe_execution(
            executionArn=self.execution_arn(state_machine_name, execution_name))

    def get_execution_history(self, state_machine_name, execution_name, max_results=10):
        """
        Get the execution history

        By default, this method only retrieves the most recent events of the
        execution. However, when the argument ``max_results`` is ZERO, this
        method will retrieve the whole history.
        """
        events = []
        params = dict(
            executionArn=self.execution_arn(state_machine_name, execution_name),
            reverseOrder=True
        )
        if max_results > 0:
            params['maxResults'] = max_results
        while True:
            history = aws.stepfunctions.get_execution_history(**params)
            events.extend(history['events'])
            if 'maxResults' in params:
                break
            if history.get('nextToken') is not None:
                params['nextToken'] = history['nextToken']
            else:
                break
        return events


class StateMachineError(RuntimeError):

    def __init__(self, *args) -> None:
        super().__init__('Failed to generate manifest', *args)
