import base64
from enum import (
    Enum,
    auto,
)
import json
import logging
from typing import (
    Tuple,
    Union,
)

import attr
from botocore.exceptions import (
    ClientError,
)

from azul.enums import (
    CaseInsensitiveEnumMeta,
)
from azul.service import (
    AbstractService,
)
from azul.service.step_function_helper import (
    StateMachineError,
    StepFunctionHelper,
)
from azul.types import (
    JSON,
)

logger = logging.getLogger(__name__)


class Status(Enum, metaclass=CaseInsensitiveEnumMeta):
    running = auto()
    succeeded = auto()
    failed = auto()
    timed_out = auto()
    aborted = auto()


class InvalidTokenError(Exception):

    def __init__(self) -> None:
        super().__init__('Invalid token given')


@attr.s(frozen=True, auto_attribs=True, kw_only=True)
class Token:
    """
    Represents an ongoing manifest generation
    """
    execution_id: str
    request_index: int
    wait_time: int

    def encode(self) -> str:
        token = attr.asdict(self)
        return base64.urlsafe_b64encode(json.dumps(token).encode()).decode()

    @classmethod
    def decode(cls, token: str) -> 'Token':
        try:
            return cls(**json.loads(base64.urlsafe_b64decode(token).decode()))
        except Exception as e:
            raise InvalidTokenError from e

    def advance(self, wait_time: int) -> 'Token':
        return attr.evolve(self,
                           wait_time=wait_time,
                           request_index=self.request_index + 1)


class AsyncManifestService(AbstractService):
    """
    Starting and checking the status of manifest generation jobs.
    """
    helper = StepFunctionHelper()

    step_function_execution_retries = 3

    def __init__(self, state_machine_name):
        self.state_machine_name = state_machine_name

    def start_generation(self, input: JSON, execution_id: str) -> Token:

        for retry_index in range(self.step_function_execution_retries):
            token = Token(execution_id=f'{execution_id}_{retry_index}',
                          request_index=0,
                          wait_time=self._get_next_wait_time(0))
            try:
                self.helper.start_execution(self.state_machine_name,
                                            token.execution_id,
                                            execution_input=input)
            except ClientError as e:
                if e.response['Error']['Code'] == 'ExecutionAlreadyExists':
                    output, status = self._inspect_generation(token)
                    if status is Status.running:
                        return token.advance(wait_time=self._get_next_wait_time(token.request_index))
                    else:
                        # If a manifest expires before the name of the Step
                        # Function execution that created it can be reused
                        # (90 days), StartExecution will fail with
                        # ExecutionAlreadyExists. In that case we need to retry
                        # even a successful execution, so that it can recreate
                        # the manifest.
                        assert status in Status, status
                else:
                    raise
            else:
                return token
        raise StateMachineError('Maximum manifest generation retries reached',
                                execution_id)

    def inspect_generation(self, token) -> Union[Token, JSON]:
        output, status = self._inspect_generation(token)
        if status is Status.succeeded:
            # Because describe_execution is eventually consistent, output may
            # not yet be present
            if output is None:
                return token.advance(wait_time=1)
            else:
                return json.loads(output)
        elif status is Status.running:
            return token.advance(wait_time=self._get_next_wait_time(token.request_index))
        else:
            raise StateMachineError(status, output)

    def _inspect_generation(self, token: Token) -> Tuple[str, Status]:
        try:
            name = self.state_machine_name
            execution = self.helper.describe_execution(state_machine_name=name,
                                                       execution_name=token.execution_id)
        except ClientError as e:
            if e.response['Error']['Code'] == 'ExecutionDoesNotExist':
                raise InvalidTokenError from e
            else:
                raise
        output = execution.get('output')
        status = Status[execution['status']]
        return output, status

    def _get_next_wait_time(self, request_index: int) -> int:
        wait_times = [1, 1, 4, 6, 10]
        try:
            return wait_times[request_index]
        except IndexError:
            return wait_times[-1]
