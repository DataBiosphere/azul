from collections import (
    Counter,
)
import logging
from typing import (
    Dict,
)

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
from azul.plugins.repository.tdr import (
    TDRSourceRef,
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


def verify_sources(source_names_by_id: Dict[str, str]):
    dupes = {
        name
        for name, count in Counter(source_names_by_id.values()).items()
        if count > 1
    }
    require(not dupes, 'Snapshot names are ambiguous', dupes)
    ids_by_name = {name: id for id, name in source_names_by_id.items()}
    tdr_sources = {
        TDRSourceRef(id=ids_by_name[spec.name], spec=spec)
        for spec in (
            TDRSourceSpec.parse(source)
            for catalog in config.catalogs.values()
            if catalog.plugins[RepositoryPlugin.type_name()].name == 'tdr'
            for source in config.tdr_sources(catalog.name)
        )
    }
    assert tdr_sources, tdr_sources
    for source in tdr_sources:
        verify_source(source)


def verify_source(source_ref: TDRSourceRef):
    tdr.check_bigquery_access(source_ref.spec)
    source = tdr.check_api_access(source_ref.spec, source_ref.id)
    require(source.project == source_ref.spec.project,
            'Actual Google project of TDR source differs from configured one',
            source.project, source_ref.spec.project)
    # Uppercase is standard for multi-regions in the documentation but TDR
    # returns 'us' in lowercase
    require(source.location.lower() == config.tdr_source_location.lower(),
            'Actual storage location of TDR source differs from configured one',
            source.location, config.tdr_source_location)


def verify_source_access() -> Dict[str, str]:
    public_snapshots = public_tdr.snapshot_names_by_id()
    all_snapshots = tdr.snapshot_names_by_id()
    diff = public_snapshots.keys() - all_snapshots.keys()
    require(not diff,
            'The public service account can access snapshots that the indexer '
            'service account cannot', diff)
    return all_snapshots


def main():
    configure_script_logging(log)
    register_with_sam()
    sources = verify_source_access()
    verify_sources(sources)


if __name__ == '__main__':
    main()
