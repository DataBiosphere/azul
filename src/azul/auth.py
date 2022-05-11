import abc
from inspect import (
    isabstract,
)
from typing import (
    ClassVar,
    Type,
)

import attr

from azul import (
    JSON,
)
from azul.json import (
    copy_json,
)


@attr.s(auto_attribs=True, frozen=True)
class Authentication(abc.ABC):

    @abc.abstractmethod
    def identity(self) -> str:
        """
        A string uniquely identifying the authenticated entity, for at least
        some period of time.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def as_http_header(self) -> str:
        """
        A string representing the authenticated entity as an HTTP header
        name/value pair. Raises NotImplementedError if the authentication format
        does not support such a representation.
        """
        raise NotImplementedError

    _cls_field: ClassVar[str] = '_cls'

    def to_json(self) -> JSON:
        """
        >>> @attr.s(auto_attribs=True, frozen=True)
        ... class Foo(Authentication):
        ...     foo: str
        ...     def identity(self) -> str:
        ...         # noinspection PyUnresolvedReferences
        ...         return self.foo
        ...     def as_http_header(self) -> str:
        ...         raise NotImplementedError
        >>> f = Foo('bar')
        >>> f
        Foo(foo='bar')
        >>> f.to_json()
        {'foo': 'bar', '_cls': 'Foo'}
        >>> Authentication.from_json(f.to_json())
        Foo(foo='bar')
        """
        json = attr.asdict(self)
        json[self._cls_field] = type(self).__name__
        return json

    @classmethod
    def from_json(cls, json: JSON) -> 'Authentication':
        json = copy_json(json)
        cls_name = json.pop(cls._cls_field)
        return cls._cls_for_name[cls_name](**json)

    _cls_for_name: ClassVar[dict[str, Type['Authentication']]] = {}

    def __init_subclass__(cls) -> None:
        super().__init_subclass__()
        if not isabstract(cls):
            name = cls.__name__
            assert name not in cls._cls_for_name, cls
            assert cls._cls_field not in attr.fields_dict(cls), cls
            cls._cls_for_name[name] = cls


@attr.s(auto_attribs=True, frozen=True)
class OAuth2(Authentication):
    access_token: str

    def identity(self) -> str:
        return self.access_token

    def as_http_header(self) -> str:
        return f'Authorization: Bearer {self.access_token}'


@attr.s(auto_attribs=True, frozen=True)
class HMACAuthentication(Authentication):
    key_id: str

    def identity(self) -> str:
        return self.key_id

    def as_http_header(self) -> str:
        raise NotImplementedError
