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

    def start_execution(self, state_machine_name, execution_name=None, execution_input=None):
        """
        Start an execution of a state machine
        Wrapper around https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/stepfunctions.html#SFN.Client.start_execution

        :param state_machine_name: name of state machine to run
        :param execution_name: name to give execution; random uuid is used if name is not given
        :param execution_input: Input to execution that will be JSON-encoded
        :return: Dict with structure
            {
                'executionArn': 'string',
                'startDate': datetime(2015, 1, 1)
            }
        """
        execution_params = {
            'stateMachineArn': self.state_machine_arn(state_machine_name),
            'input': json.dumps(execution_input)
        }
        if execution_name is not None:
            execution_params['name'] = execution_name
        return aws.stepfunctions.start_execution(**execution_params)

    def describe_execution(self, state_machine_name, execution_name):
        """
        Wrapper around https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/stepfunctions.html#SFN.Client.describe_execution

        :return: Dict with structure
            {
                'executionArn': 'string',
                'stateMachineArn': 'string',
                'name': 'string',
                'status': 'RUNNING'|'SUCCEEDED'|'FAILED'|'TIMED_OUT'|'ABORTED',
                'startDate': datetime(2015, 1, 1),
                'stopDate': datetime(2015, 1, 1),
                'input': 'string',
                'output': 'string'
            }
        """
        return aws.stepfunctions.describe_execution(
            executionArn=self.execution_arn(state_machine_name, execution_name))


class StateMachineError(BaseException):
    pass
