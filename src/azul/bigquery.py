from collections.abc import (
    Iterable,
    Mapping,
)
from datetime import (
    datetime,
)
import re
from typing import (
    Union,
)

from azul import (
    require,
)

BigQueryValue = Union[int, float, bool, str, bytes, datetime, None]
BigQueryRow = Mapping[str, BigQueryValue]
BigQueryRows = Iterable[BigQueryRow]

identifier_re = r'([a-zA-Z_][a-zA-Z_0-9]*)'
table_name_re = re.compile(fr'{identifier_re}(\.{identifier_re})*')


def backtick(table_name: str) -> str:
    """
    Return the given string surrounded by backticks if deemed necessary based
    on a simplified interpretation of BigQuery's lexical structure and syntax
    for identifier tokens.

    https://cloud.google.com/bigquery/docs/reference/standard-sql/lexical

    >>> backtick('foo.bar.my_table')
    'foo.bar.my_table'

    >>> backtick('foo2.bar.my_table')
    'foo2.bar.my_table'

    >>> backtick('foo-2.bar.my_table')
    '`foo-2.bar.my_table`'

    >>> backtick('foo-2.bar`s.my_table')
    Traceback (most recent call last):
    ...
    azul.RequirementError: foo-2.bar`s.my_table
    """
    if table_name_re.fullmatch(table_name):
        return table_name
    else:
        require('`' not in table_name, table_name)
        return f'`{table_name}`'
