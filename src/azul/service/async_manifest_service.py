import base64
import json
import logging
from typing import (
    Union,
)
import uuid

import attrs
from botocore.exceptions import (
    ClientError,
)
import msgpack

from azul.attrs import (
    strict_auto,
)
from azul.deployment import (
    aws,
)
from azul.types import (
    JSON,
)

logger = logging.getLogger(__name__)


@attrs.frozen
class InvalidTokenError(Exception):
    value: str


@attrs.frozen(kw_only=True)
class Token:
    """
    Represents an ongoing manifest generation
    """
    execution_id: str = strict_auto()
    request_index: int = strict_auto()
    wait_time: int = strict_auto()

    def pack(self) -> bytes:
        return msgpack.packb([
            self.execution_id,
            self.request_index,
            self.wait_time
        ])

    @classmethod
    def unpack(cls, pack: bytes) -> 'Token':
        i = iter(msgpack.unpackb(pack))
        return cls(execution_id=next(i),
                   request_index=next(i),
                   wait_time=next(i))

    def encode(self) -> str:
        return base64.urlsafe_b64encode(self.pack()).decode()

    @classmethod
    def decode(cls, token: str) -> 'Token':
        try:
            return cls.unpack(base64.urlsafe_b64decode(token))
        except Exception as e:
            raise InvalidTokenError(token) from e

    def advance(self, wait_time: int) -> 'Token':
        return attrs.evolve(self,
                            wait_time=wait_time,
                            request_index=self.request_index + 1)


@attrs.frozen
class NoSuchGeneration(Exception):
    token: Token


class StateMachineError(RuntimeError):

    def __init__(self, *args) -> None:
        super().__init__('Failed to generate manifest', *args)


class AsyncManifestService:
    """
    Starting and checking the status of manifest generation jobs.
    """

    def __init__(self, state_machine_name):
        self.state_machine_name = state_machine_name

    def start_generation(self, input: JSON) -> Token:
        execution_id = str(uuid.uuid4())
        machine_arn = self.state_machine_arn(self.state_machine_name)
        response = self._sfn.start_execution(stateMachineArn=machine_arn,
                                             name=execution_id,
                                             input=json.dumps(input))
        execution_arn = self.execution_arn(self.state_machine_name, execution_id)
        assert execution_arn == response['executionArn']
        return Token(execution_id=execution_id,
                     request_index=0,
                     wait_time=self._get_next_wait_time(0))

    def inspect_generation(self, token: Token) -> Union[Token, JSON]:
        try:
            execution_arn = self.execution_arn(self.state_machine_name, token.execution_id)
            execution = self._sfn.describe_execution(executionArn=execution_arn)
        except ClientError as e:
            if e.response['Error']['Code'] == 'ExecutionDoesNotExist':
                raise NoSuchGeneration(token)
            else:
                raise
        output = execution.get('output', None)
        status = execution['status']
        if status == 'SUCCEEDED':
            # Because describe_execution is eventually consistent, output may
            # not yet be present
            if output is None:
                return token.advance(wait_time=1)
            else:
                return json.loads(output)
        elif status == 'RUNNING':
            return token.advance(wait_time=self._get_next_wait_time(token.request_index))
        else:
            raise StateMachineError(status, output)

    def _get_next_wait_time(self, request_index: int) -> int:
        wait_times = [1, 1, 4, 6, 10]
        try:
            return wait_times[request_index]
        except IndexError:
            return wait_times[-1]

    @classmethod
    def arn(cls, suffix):
        return f'arn:aws:states:{aws.region_name}:{aws.account}:{suffix}'

    @classmethod
    def state_machine_arn(cls, state_machine_name):
        return cls.arn(f'stateMachine:{state_machine_name}')

    @classmethod
    def execution_arn(cls, state_machine_name, execution_name):
        return cls.arn(f'execution:{state_machine_name}:{execution_name}')

    @property
    def _sfn(self):
        return aws.stepfunctions
