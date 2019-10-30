from typing import (
    Union,
    Mapping,
    Any,
    Sequence,
    MutableMapping,
    MutableSequence,
)

PrimitiveJSON = Union[str, int, float, bool, None]

AnyJSON4 = Union[Mapping[str, Any], Sequence[Any], PrimitiveJSON]
AnyJSON3 = Union[Mapping[str, AnyJSON4], Sequence[AnyJSON4], PrimitiveJSON]
AnyJSON2 = Union[Mapping[str, AnyJSON3], Sequence[AnyJSON3], PrimitiveJSON]
AnyJSON1 = Union[Mapping[str, AnyJSON2], Sequence[AnyJSON2], PrimitiveJSON]
AnyJSON = Union[Mapping[str, AnyJSON1], Sequence[AnyJSON1], PrimitiveJSON]
JSON = Mapping[str, AnyJSON]
JSONs = Sequence[JSON]

AnyMutableJSON4 = Union[MutableMapping[str, Any], MutableSequence[Any], PrimitiveJSON]
AnyMutableJSON3 = Union[MutableMapping[str, AnyMutableJSON4], MutableSequence[AnyMutableJSON4], PrimitiveJSON]
AnyMutableJSON2 = Union[MutableMapping[str, AnyMutableJSON3], MutableSequence[AnyMutableJSON3], PrimitiveJSON]
AnyMutableJSON1 = Union[MutableMapping[str, AnyMutableJSON2], MutableSequence[AnyMutableJSON2], PrimitiveJSON]
AnyMutableJSON = Union[MutableMapping[str, AnyMutableJSON1], MutableSequence[AnyMutableJSON1], PrimitiveJSON]
MutableJSON = MutableMapping[str, AnyMutableJSON]
MutableJSONs = MutableSequence[MutableJSON]


class LambdaContext(object):
    """
    A stub for the AWS Lambda context
    """

    @property
    def aws_request_id(self) -> str:
        raise NotImplementedError

    @property
    def log_group_name(self) -> str:
        raise NotImplementedError

    @property
    def log_stream_name(self) -> str:
        raise NotImplementedError

    @property
    def function_name(self) -> str:
        raise NotImplementedError

    @property
    def memory_limit_in_mb(self) -> str:
        raise NotImplementedError

    @property
    def function_version(self) -> str:
        raise NotImplementedError

    @property
    def invoked_function_arn(self) -> str:
        raise NotImplementedError

    def get_remaining_time_in_millis(self) -> int:
        raise NotImplementedError

    def log(self, msg: str) -> None:
        raise NotImplementedError
