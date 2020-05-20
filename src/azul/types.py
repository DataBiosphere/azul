from typing import (
    Any,
    Dict,
    List,
    Mapping,
    Sequence,
    Union,
)

PrimitiveJSON = Union[str, int, float, bool, None]

# Not every instance of Mapping or Sequence can be fed to json.dump() but those
# two generic types are the most specific *immutable* super-types of `list`,
# `tuple` and `dict`:

AnyJSON4 = Union[Mapping[str, Any], Sequence[Any], PrimitiveJSON]
AnyJSON3 = Union[Mapping[str, AnyJSON4], Sequence[AnyJSON4], PrimitiveJSON]
AnyJSON2 = Union[Mapping[str, AnyJSON3], Sequence[AnyJSON3], PrimitiveJSON]
AnyJSON1 = Union[Mapping[str, AnyJSON2], Sequence[AnyJSON2], PrimitiveJSON]
AnyJSON = Union[Mapping[str, AnyJSON1], Sequence[AnyJSON1], PrimitiveJSON]
JSON = Mapping[str, AnyJSON]
JSONs = Sequence[JSON]

# For mutable JSON we can be more specific and use Dict and List:

AnyMutableJSON4 = Union[Dict[str, Any], List[Any], PrimitiveJSON]
AnyMutableJSON3 = Union[Dict[str, AnyMutableJSON4], List[AnyMutableJSON4], PrimitiveJSON]
AnyMutableJSON2 = Union[Dict[str, AnyMutableJSON3], List[AnyMutableJSON3], PrimitiveJSON]
AnyMutableJSON1 = Union[Dict[str, AnyMutableJSON2], List[AnyMutableJSON2], PrimitiveJSON]
AnyMutableJSON = Union[Dict[str, AnyMutableJSON1], List[AnyMutableJSON1], PrimitiveJSON]
MutableJSON = Dict[str, AnyMutableJSON]
MutableJSONs = List[MutableJSON]


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
