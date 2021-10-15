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
from azul.plugins import (
    RepositoryPlugin,
)
from azul.terra import (
    TDRClient,
    TDRSourceSpec,
)

log = logging.getLogger(__name__)

tdr = TDRClient.with_service_account_credentials()
public_tdr = TDRClient.with_public_service_account_credentials()


def register_with_sam():
    tdr.register_with_sam()
    public_tdr.register_with_sam()


def verify_sources():
    tdr_catalogs = {
        catalog.name
        for catalog in config.catalogs.values()
        if catalog.plugins[RepositoryPlugin.type_name()].name == 'tdr'
    }
    assert tdr_catalogs, tdr_catalogs
    for source in set(chain(*map(config.sources, tdr_catalogs))):
        source = TDRSourceSpec.parse(source)
        verify_source(source)


def verify_source(source_spec: TDRSourceSpec):
    tdr.check_api_access(source_spec)
    tdr.check_bigquery_access(source_spec)
    source = tdr.lookup_source(source_spec)
    require(source.project == source_spec.project,
            'Actual Google project of TDR source differs from configured one',
            source.project, source_spec.project)
    # Uppercase is standard for multi-regions in the documentation but TDR
    # returns 'us' in lowercase
    require(source.location.lower() == config.tdr_source_location.lower(),
            'Actual storage location of TDR source differs from configured one',
            source.location, config.tdr_source_location)


def verify_source_access():
    public_snapshots = set(public_tdr.snapshot_names_by_id())
    all_snapshots = set(tdr.snapshot_names_by_id())
    diff = public_snapshots - all_snapshots
    require(not diff,
            'The public service account can access snapshots that the indexer '
            'service account cannot', diff)


def main():
    configure_script_logging(log)
    register_with_sam()
    verify_sources()
    verify_source_access()


if __name__ == '__main__':
    main()
