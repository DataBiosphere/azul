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
        """
        Wrapper around https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/stepfunctions.html#SFN.Client.start_execution
        """
        execution_params = {
            'stateMachineArn': self.state_machine_arn(state_machine_name),
            'name': execution_name,
            'input': json.dumps(execution_input)
        }
        execution_response = aws.stepfunctions.start_execution(**execution_params)
        assert self.execution_arn(state_machine_name, execution_name) == execution_response['executionArn']

    def describe_execution(self, state_machine_name, execution_name):
        """
        Wrapper around https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/stepfunctions.html#SFN.Client.describe_execution
        """
        return aws.stepfunctions.describe_execution(
            executionArn=self.execution_arn(state_machine_name, execution_name))


class StateMachineError(BaseException):
    def __init__(self, msg):
        self.msg = msg
