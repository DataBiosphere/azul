from datetime import (
    datetime,
)
from typing import (
    Iterable,
    Mapping,
    Union,
)

BigQueryValue = Union[int, float, bool, str, bytes, datetime, None]
BigQueryRow = Mapping[str, BigQueryValue]
BigQueryRows = Iterable[BigQueryRow]
