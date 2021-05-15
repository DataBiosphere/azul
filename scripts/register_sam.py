from itertools import (
    chain,
)
import logging

from azul import (
    config,
    require,
)
from azul.logging import (
    configure_script_logging,
)
from azul.terra import (
    TDRClient,
    TDRSourceSpec,
)

log = logging.getLogger(__name__)


def main():
    configure_script_logging(log)
    tdr = TDRClient.with_service_account_credentials()
    tdr.register_with_sam()

    public_tdr = TDRClient.with_public_service_account_credentials()
    public_tdr.register_with_sam()

    tdr_catalogs = (
        catalog.name
        for catalog in config.catalogs.values()
        if catalog.plugins['repository'] == 'tdr'
    )
    for source in set(chain(*map(config.tdr_sources, tdr_catalogs))):
        source = TDRSourceSpec.parse(source)
        api_project = tdr.lookup_source_project(source)
        require(api_project == source.project,
                'Actual Google project of TDR source differs from configured '
                'one',
                api_project, source)
        tdr.check_api_access(source)
        tdr.check_bigquery_access(source)

    public_snapshots = set(public_tdr.snapshot_names_by_id())
    all_snapshots = set(tdr.snapshot_names_by_id())
    diff = public_snapshots - all_snapshots
    require(not diff,
            'The public service account can access snapshots that the indexer '
            'service account cannot: %r', diff)


if __name__ == '__main__':
    main()
