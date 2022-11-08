"""
Fetch AWS SecurityHub findings and export to a CSV file.
"""
import csv
import json
import logging
import sys
from typing import (
    Any,
    Optional,
    Union,
)

from azul import (
    config,
)
from azul.deployment import (
    aws,
)
from azul.logging import (
    configure_script_logging,
)
from azul.types import (
    JSON,
    JSONs,
    MutableJSON,
    PrimitiveJSON,
)

log = logging.getLogger(__name__)

pagination_page_size = 100


def dict_append_value(d: MutableJSON, k: str, v: Any) -> None:
    """
    Adds a value to a dictionary at the specified key. If the key already exists
    in the dictionary, the existing value is converted to a list and the new
    value is appended.
    """
    if k in d:
        if not isinstance(d[k], list):
            d[k] = [d[k]]
        if isinstance(v, list):
            for v2 in v:
                dict_append_value(d, k, v2)
        else:
            d[k].append(v)
    else:
        d[k] = v


def format_row(obj: Union[JSON, JSONs, PrimitiveJSON],
               keys: Optional[tuple[str]] = None
               ) -> dict[str, Any]:
    """
    Returns a flat, non-nested dictionary version of the given object.
    Note: When the value is a list of dictionaries it will be converted to a
    list of string representations.
    """
    assert isinstance(obj, dict) or keys, 'Must start with a dictionary'
    if keys is None:
        keys = tuple()
    flat = {}
    if isinstance(obj, dict):
        for k, v in obj.items():
            for k2, v2 in format_row(v, keys + (k,)).items():
                dict_append_value(flat, k2, v2)
    elif isinstance(obj, list):
        for v in obj:
            if isinstance(v, dict):
                # Don't further flatten when we have a list of dictionaries
                dict_append_value(flat, '_'.join(keys), v)
            else:
                for k2, v2 in format_row(v, keys).items():
                    dict_append_value(flat, k2, v2)
    else:
        flat.update({'_'.join(keys): obj})
    return flat


def fetch_findings() -> JSONs:
    """
    Fetch all AWS SecurityHub findings.
    """
    log.info('Fetching SecurityHub findings from %r…', config.deployment_stage)

    rows = []  # A list of nested dict SecurityHub findings
    paginator = aws.securityhub.get_paginator('get_findings')
    pagination = paginator.paginate(MaxResults=pagination_page_size,
                                    SortCriteria=[{'Field': 'SeverityNormalized', 'SortOrder': 'desc'},
                                                  {'Field': 'SeverityLabel', 'SortOrder': 'desc'},
                                                  {'Field': 'SeverityProduct', 'SortOrder': 'desc'},
                                                  {'Field': 'UpdatedAt', 'SortOrder': 'desc'}])
    for i, page in enumerate(pagination):
        metadata = page['ResponseMetadata']
        assert metadata['HTTPStatusCode'] == 200, metadata
        log.info('Fetched page %i with %i findings.', i + 1, len(page['Findings']))
        for row in page['Findings']:
            rows.append(row)
    return rows


def format_findings(rows: JSONs) -> JSONs:
    """
    Takes a list of nested JSON and returns a list of non-nested JSON.
    """
    formatted_rows = []  # A list of flattened dict SecurityHub findings
    all_keys = list(format_row(rows[0]).keys())  # preserve ordering with a list
    for row in rows:
        formatted_row = format_row(row)
        formatted_rows.append(formatted_row)
        for key in formatted_row.keys() - set(all_keys):
            log.info('Adding extra column %r to all rows…', key)
            all_keys.append(key)
    # Make sure all rows have all the columns
    for formatted_row in formatted_rows:
        for key in all_keys - formatted_row.keys():
            formatted_row[key] = ''
    return formatted_rows


def write_json(obj: JSONs, suffix: str) -> None:
    """
    Write a list of JSON objects to a JSON file.
    """
    output_filename = f'securityhub_{config.deployment_stage}_{suffix}.json'
    log.info('Writing %i rows to %r…', len(obj), output_filename)
    with open(output_filename, 'w') as f:
        json.dump(obj, f)


def write_csv(obj: JSONs) -> None:
    """
    Write a list of JSON objects to a CSV file.
    """
    output_filename = f'securityhub_{config.deployment_stage}.csv'
    log.info('Writing %i rows to %r…', len(obj), output_filename)
    with open(output_filename, 'w') as f:
        writer = csv.writer(f, quoting=csv.QUOTE_NONNUMERIC)
        keys = list(obj[0].keys())
        writer.writerow(keys)
        for row in obj:
            writer.writerow([row[k] for k in keys])


def main() -> int:
    rows = fetch_findings()
    if config.debug:
        write_json(rows, suffix='raw')
    if rows:
        formatted_rows = format_findings(rows)
        if config.debug:
            write_json(formatted_rows, suffix='formatted')
        write_csv(formatted_rows)
        log.info('Done.')
    else:
        log.info('No data found.')
    return 0


if __name__ == '__main__':
    configure_script_logging(log)
    sys.exit(main())
