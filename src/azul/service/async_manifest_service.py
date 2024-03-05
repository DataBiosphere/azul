import base64
import json
import logging
from typing import (
    Optional,
    Self,
    Union,
)

import attrs
import msgpack

from azul import (
    config,
    require,
)
from azul.attrs import (
    strict_auto,
)
from azul.bytes import (
    azul_urlsafe_b64encode,
)
from azul.deployment import (
    aws,
)
from azul.types import (
    JSON,
)

log = logging.getLogger(__name__)


@attrs.frozen
class InvalidTokenError(Exception):
    value: str = strict_auto()


@attrs.frozen(kw_only=True)
class Token:
    """
    Represents an ongoing manifest generation
    """
    execution_id: bytes = strict_auto()
    request_index: int = strict_auto()
    retry_after: int = strict_auto()

    def pack(self) -> bytes:
        return msgpack.packb([
            self.execution_id,
            self.request_index,
            self.retry_after
        ])

    @classmethod
    def unpack(cls, pack: bytes) -> Self:
        i = iter(msgpack.unpackb(pack))
        return cls(execution_id=next(i),
                   request_index=next(i),
                   retry_after=next(i))

    def encode(self) -> str:
        return base64.urlsafe_b64encode(self.pack()).decode()

    @classmethod
    def decode(cls, token: str) -> Self:
        try:
            return cls.unpack(base64.urlsafe_b64decode(token))
        except Exception as e:
            raise InvalidTokenError(token) from e

    @classmethod
    def first(cls, execution_id: bytes) -> Self:
        return cls(execution_id=execution_id,
                   request_index=0,
                   retry_after=cls._next_retry_after(0))

    def next(self, *, retry_after: Optional[int] = None) -> Self:
        if retry_after is None:
            retry_after = self._next_retry_after(self.request_index)
        return attrs.evolve(self,
                            retry_after=retry_after,
                            request_index=self.request_index + 1)

    @classmethod
    def _next_retry_after(cls, request_index: int) -> int:
        delays = [1, 1, 4, 6, 10]
        try:
            return delays[request_index]
        except IndexError:
            return delays[-1]


@attrs.frozen
class NoSuchGeneration(Exception):
    token: Token = strict_auto()


@attrs.frozen(kw_only=True)
class GenerationFailed(Exception):
    status: str = strict_auto()
    output: Optional[str] = strict_auto()


@attrs.frozen
class InvalidGeneration(Exception):
    token: Token = strict_auto()


class AsyncManifestService:
    """
    Starting and checking the status of manifest generation jobs.
    """

    @property
    def machine_name(self):
        return config.qualified_resource_name(config.manifest_sfn)

    def start_generation(self, execution_id: bytes, input: JSON) -> Token:
        execution_name = self.execution_name(execution_id)
        execution_arn = self.execution_arn(execution_name)
        # The input contains the verbatim manifest key as JSON while the ARN
        # contains the encoded hash of the key so this log line is useful for
        # associating the hash with the key for diagnostic purposes
        log.info('Starting execution %r for input %r', execution_arn, input)
        token = Token.first(execution_id)
        input = json.dumps(input)
        try:
            # If there already is an execution of the given name, and if that
            # execution is still ongoing and was given the same input as what we
            # pass here, `start_execution` will succeed idempotently
            execution = self._sfn.start_execution(stateMachineArn=self.machine_arn,
                                                  name=execution_name,
                                                  input=input)
        except self._sfn.exceptions.ExecutionAlreadyExists:
            # This exception indicates that there is already an execution with
            # the given name but that it has ended, or that its input differs
            # from what we were passing now. The latter case is unexpected and
            # therefore constitues an error. In the former case we return the
            # token so that the client has to make another request to actually
            # obtain the resulting manifest. Strictly speaking, we could return
            # the manifest here, but it keeps the control flow simpler. This
            # benevolent race is rare enough to not worry about optimizing.
            execution = self._sfn.describe_execution(executionArn=execution_arn)
            if execution['input'] != input:
                raise InvalidGeneration(token)
            else:
                log.info('A completed execution %r already exists', execution_arn)
                return token
        else:
            assert execution_arn == execution['executionArn'], execution
            log.info('Started execution %r or it was already running', execution_arn)
            return token

    def inspect_generation(self, token: Token) -> Union[Token, JSON]:
        execution_name = self.execution_name(token.execution_id)
        execution_arn = self.execution_arn(execution_name)
        try:
            execution = self._sfn.describe_execution(executionArn=execution_arn)
        except self._sfn.exceptions.ExecutionDoesNotExist:
            raise NoSuchGeneration(token)
        else:
            output = execution.get('output')
            status = execution['status']
            if status == 'SUCCEEDED':
                if output is None:
                    log.info('Execution %r succeeded, no output yet', execution_arn)
                    return token.next(retry_after=1)
                else:
                    log.info('Execution %r succeeded with output %r', execution_arn, output)
                    return json.loads(output)
            elif status == 'RUNNING':
                log.info('Execution %r is still running', execution_arn)
                return token.next()
            else:
                raise GenerationFailed(status=status, output=output)

    def arn(self, suffix):
        return f'arn:aws:states:{aws.region_name}:{aws.account}:{suffix}'

    @property
    def machine_arn(self):
        return self.arn(f'stateMachine:{self.machine_name}')

    def execution_arn(self, execution_name):
        return self.arn(f'execution:{self.machine_name}:{execution_name}')

    def execution_name(self, execution_id: bytes) -> str:
        require(0 < len(execution_id) <= 60,
                'Execution ID is too short or too long', execution_id)
        execution_name = azul_urlsafe_b64encode(execution_id)
        assert 0 < len(execution_name) <= 80, (execution_id, execution_name)
        return execution_name

    @property
    def _sfn(self):
        return aws.stepfunctions
