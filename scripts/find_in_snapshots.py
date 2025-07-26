"""
Command line utility to validate snapshots prior to indexing by Azul
"""

import argparse
from collections import (
    defaultdict,
)
import json
import logging
import sys

from azul import (
    config,
)
from azul.args import (
    AzulArgumentHelpFormatter,
)
from azul.azulclient import (
    AzulClient,
)
from azul.bigquery import (
    backtick,
)
from azul.logging import (
    configure_script_logging,
)
from azul.plugins.repository.tdr import (
    TDRPlugin,
)
from azul.terra import (
    TDRSourceSpec,
)

log = logging.getLogger(__name__)
configure_script_logging(log)


def main(args):
    declined_snapshots = list()

    azul = AzulClient(num_workers=1)
    sources_by_catalog = azul.sources_by_catalog(args.catalogs)
    previous_sources: set[str] = set()
    for catalog, sources in sources_by_catalog.items():
        sources -= previous_sources
        tdr_plugin = azul.repository_plugin(catalog)
        assert isinstance(tdr_plugin, TDRPlugin)
        log.info('Checking for %r in catalog %s', args.match, catalog)
        if args.snapshot is not None:
            sources = {s for s in sources if args.snapshot in s}
        for spec in sources:
            log.info('Validating snapshot %s', spec)
            source = TDRSourceSpec.parse(spec)
            tables = tdr_plugin._full_table_name(source, 'INFORMATION_SCHEMA.COLUMNS')
            query = f'SELECT table_name, column_name FROM {backtick(tables)}'
            rows = tdr_plugin._run_sql(query)
            table_columns = defaultdict(list)
            for row in rows:
                table_name, column_name = row['table_name'], row['column_name']
                assert isinstance(table_name, str), table_name
                assert isinstance(column_name, str), column_name
                table_columns[table_name].append(column_name)
            for table_name, columns in table_columns.items():
                log.info('Validating table %s', table_name)
                table = tdr_plugin._full_table_name(source, table_name)
                for column in columns:
                    query = f'''
                            SELECT datarepo_row_id, {column}
                            FROM {backtick(table)}
                            WHERE CONTAINS_SUBSTR({column}, '{args.match}')
                            '''
                    result = tdr_plugin._run_sql(query)
                    for row in result:
                        match = {
                            'catalog': catalog,
                            'spec': spec,
                            'table': table,
                            'column': column,
                            'row_id': row['datarepo_row_id'],
                            'value': row[column]
                        }
                        log.warning('Undesired string found: %r', match)
                        declined_snapshots.append(match)
        previous_sources = sources
    print()
    if declined_snapshots:
        print(json.dumps(declined_snapshots, indent=4))
    else:
        print('Checked snapshots OK')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=AzulArgumentHelpFormatter)
    parser.add_argument('--catalogs',
                        nargs='+',
                        metavar='CATALOG',
                        default=[
                            catalog.name
                            for catalog in config.catalogs.values()
                            if not catalog.is_integration_test_catalog
                        ]
                        if config.current_catalog is None else
                        [
                            config.catalogs[config.current_catalog].name
                        ],
                        choices=config.catalogs,
                        help='The names of the catalogs to validate.')
    parser.add_argument('--match',
                        metavar='STR_MATCH',
                        default='||',
                        help='The string pattern to match.')
    parser.add_argument('--snapshot',
                        required=False,
                        metavar='SNAPSHOT_SEQ',
                        help='Limit scan to matching string sequence in selected catalog(s).')

    args = parser.parse_args(sys.argv[1:])
    main(args)
