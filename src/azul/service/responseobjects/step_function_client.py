import json

from azul.deployment import aws


class StepFunctionClient:
    """
    Wrapper around boto3 SFN client to handle resource name generation
    """
    @classmethod
    def state_machine_arn(cls, state_machine_name):
        return f'arn:aws:states:{aws.region_name}:{aws.account}:stateMachine:{state_machine_name}'

    @classmethod
    def execution_arn(cls, state_machine_name, execution_name):
        return f'arn:aws:states:{aws.region_name}:{aws.account}:execution:{state_machine_name}:{execution_name}'

    @classmethod
    def start_execution(cls, state_machine_name, execution_name=None, execution_input=None):
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
            'stateMachineArn': cls.state_machine_arn(state_machine_name),
            'input': json.dumps(execution_input)
        }
        if execution_name is not None:
            execution_params['name'] = execution_name
        return aws.step_functions.start_execution(**execution_params)

    @classmethod
    def describe_execution(cls, state_machine_name, execution_name):
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
        return aws.step_functions.describe_execution(
            executionArn=cls.execution_arn(state_machine_name, execution_name))


class StateMachineError(BaseException):
    pass
