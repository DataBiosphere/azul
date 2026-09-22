import base64
import json
import logging
from typing import (
    Self,
    TypedDict,
)
from uuid import (
    UUID,
)

import attrs
import msgpack

from azul import (
    config,
)
from azul.deployment import (
    aws,
)
from azul.lib.attrs import (
    strict_auto,
)
from azul.lib.types import (
    JSON,
)

log = logging.getLogger(__name__)


@attrs.frozen
class InvalidTokenError(Exception):
    value: str = strict_auto()


@attrs.frozen(kw_only=True)
class Token:
    """
    Represents a Step Function execution to generate a manifest
    """
    #: A hash of the inputs
    generation_id: UUID = strict_auto()

    #: Number of prior executions for the generation represented by this token
    iteration: int = strict_auto()

    #: The number of times the service received a request to inspect the
    #: status of the execution represented by this token
    request_index: int = strict_auto()

    #: How long clients should wait before requesting a status update about the
    #: execution represented by the token
    retry_after: int = strict_auto()

    @property
    def execution_id(self) -> tuple[UUID, int]:
        return self.generation_id, self.iteration

    def pack(self) -> bytes:
        return msgpack.packb([
            self.generation_id.bytes,
            self.iteration,
            self.request_index,
            self.retry_after
        ])

    @classmethod
    def unpack(cls, pack: bytes) -> Self:
        i = iter(msgpack.unpackb(pack))
        return cls(generation_id=UUID(bytes=next(i)),
                   iteration=next(i),
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
    def first(cls, generation_id: UUID, iteration: int) -> Self:
        return cls(generation_id=generation_id,
                   iteration=iteration,
                   request_index=0,
                   retry_after=cls._next_retry_after(0))

    def next(self, *, retry_after: int | None = None) -> Self:
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


class ExecutionResult(TypedDict):
    input: JSON
    output: JSON


@attrs.frozen
class NoSuchGeneration(Exception):
    token: Token = strict_auto()


@attrs.frozen
class GenerationFinished(Exception):
    token: Token = strict_auto()


@attrs.frozen
class PreviousGenerationFailed(Exception):
    token: Token = strict_auto()


@attrs.frozen(kw_only=True)
class GenerationFailed(Exception):
    status: str = strict_auto()
    output: str | None = strict_auto()


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

    def start_generation(self,
                         generation_id: UUID,
                         input: JSON,
                         iteration: int
                         ) -> Token:
        execution_name = self.execution_name(generation_id, iteration)
        execution_arn = self.execution_arn(execution_name)
        # The input contains the verbatim manifest key as JSON while the ARN
        # contains the encoded hash of the key so this log line is useful for
        # associating the hash with the key for diagnostic purposes.
        log.info('Starting execution %r for input %r', execution_arn, input)
        token = Token.first(generation_id, iteration)
        try:
            # If there already is an execution of the given name, and if that
            # execution is still ongoing and was given the same input as what we
            # pass here, `start_execution` will succeed idempotently.
            execution = self._sfn.start_execution(stateMachineArn=self.machine_arn,
                                                  name=execution_name,
                                                  input=json.dumps(input))
        except self._sfn.exceptions.ExecutionAlreadyExists:
            # This exception indicates that there is already an execution with
            # the given name but that it has ended, or that its input differs
            # from what we were passing just now. The latter case is unexpected
            # because any part of the input that affects the output is covered
            # in the manifest hash and therefore the execution name. Any part of
            # the input not affecting the output is constant and can only change
            # with the source code which would have resulted in a different
            # execution name. The iteration is the exception that proves the
            # rule: it varies, but only between executions, and the execution
            # name names it, so two executions of the same name agree on it.
            #
            # In the former case we return the token so that the client has to
            # make another request to actually obtain the resulting manifest.
            # Strictly speaking, we could return the manifest here, but it keeps
            # the control flow simpler. This benevolent race is not probable
            # enough to warrant an optimization. If the pre-existing execution
            # failed, however, its name is poisoned: a token referring to it
            # would lead the client right back to that failure, so the caller
            # needs to know to move on to the next iteration instead.
            execution = self._sfn.describe_execution(executionArn=execution_arn)
            if input == json.loads(execution['input']):
                status = execution['status']
                if status == 'SUCCEEDED':
                    log.info('A completed execution %r already exists', execution_arn)
                    raise GenerationFinished(token)
                else:
                    log.info('A failed execution %r already exists, with status %r',
                             execution_arn, status)
                    raise PreviousGenerationFailed(token)
            else:
                raise InvalidGeneration(token)
        else:
            assert execution_arn == execution['executionArn'], (execution_arn, execution)
            log.info('Started execution %r or it was already running', execution_arn)
            return token

    def inspect_generation(self, token: Token) -> Token | ExecutionResult:
        execution_name = self.execution_name(token.generation_id, token.iteration)
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
                    return {k: json.loads(execution[k]) for k in ['input', 'output']}
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

    def execution_name(self, generation_id: UUID, iteration: int) -> str:
        assert isinstance(generation_id, UUID), generation_id
        assert isinstance(iteration, int), iteration
        execution_name = f'{generation_id}_{iteration}'
        assert 0 < len(execution_name) <= 80, execution_name
        return execution_name

    @property
    def _sfn(self):
        return aws.stepfunctions
