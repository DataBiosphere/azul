import json
import logging
from typing import (
    List,
    Mapping,
    MutableMapping,
    Optional,
    Protocol,
    Sequence,
)

import attr

from azul import (
    CatalogName,
)
from azul.types import (
    LambdaContext,
    PrimitiveJSON,
)

Filters = Mapping[str, Mapping[str, Sequence[PrimitiveJSON]]]
MutableFilters = MutableMapping[str, MutableMapping[str, List[PrimitiveJSON]]]

logger = logging.getLogger(__name__)


class BadArgumentException(Exception):

    def __init__(self, message):
        super().__init__(message)


class AbstractService:

    def parse_filters(self, filters: Optional[str]) -> MutableFilters:
        """
        Parses a string with Azul filters in JSON syntax. Handles default cases
        where filters are None or '{}'.

        :raises BadArgumentException: if input is misformatted or invalid
        """
        if filters is None:
            return {}
        else:
            return json.loads(filters)


class FileUrlFunc(Protocol):

    def __call__(self,
                 *,
                 catalog: CatalogName,
                 file_uuid: str,
                 fetch: bool = True,
                 **params: str) -> str: ...


@attr.s(auto_attribs=True, frozen=True, kw_only=True)
class Controller:
    lambda_context: LambdaContext
    file_url_func: FileUrlFunc
