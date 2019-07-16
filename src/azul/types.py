from typing import Union, Mapping, Any, List

AnyJSON3 = Union[str, int, float, bool, None, Mapping[str, Any], List[Any]]
AnyJSON2 = Union[str, int, float, bool, None, Mapping[str, AnyJSON3], List[AnyJSON3]]
AnyJSON1 = Union[str, int, float, bool, None, Mapping[str, AnyJSON2], List[AnyJSON2]]
AnyJSON = Union[str, int, float, bool, None, Mapping[str, AnyJSON1], List[AnyJSON1]]
JSON = Mapping[str, AnyJSON]


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
