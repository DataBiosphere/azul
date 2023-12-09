from concurrent.futures import (
    ThreadPoolExecutor,
    as_completed,
)
import logging

from azul import (
    CatalogName,
    config,
    require,
)
from azul.azulclient import (
    AzulClient,
)
from azul.logging import (
    configure_script_logging,
)
from azul.terra import (
    SourceRef as TDRSourceRef,
    TDRClient,
    TDRSourceSpec,
)

log = logging.getLogger(__name__)

tdr = TDRClient.for_indexer()
public_tdr = TDRClient.for_anonymous_user()

client = AzulClient()


def register_with_sam():
    tdr.register_with_sam()
    require(tdr.is_registered())
    public_tdr.register_with_sam()
    require(public_tdr.is_registered())


def verify_sources():
    tdr_catalogs = {
        catalog.name
        for catalog in config.catalogs.values()
        if config.is_tdr_enabled(catalog.name)
    }
    assert tdr_catalogs, tdr_catalogs
    futures = []
    all_sources = set()
    with ThreadPoolExecutor(max_workers=8) as tpe:
        for catalog in tdr_catalogs:
            catalog_sources = config.sources(catalog)
            for source in catalog_sources - all_sources:
                source = TDRSourceSpec.parse(source)
                futures.append(tpe.submit(verify_source, catalog, source))
            all_sources |= catalog_sources
        for completed_future in as_completed(futures):
            futures.remove(completed_future)
            e = completed_future.exception()
            if e is not None:
                for running_future in futures:
                    running_future.cancel()
                raise e


def verify_source(catalog: CatalogName, source_spec: TDRSourceSpec):
    source = tdr.lookup_source(source_spec)
    log.info('TDR client is authorized for API access to %s.', source_spec)
    require(source.project == source_spec.project,
            'Actual Google project of TDR source differs from configured one',
            source.project, source_spec.project)
    # Uppercase is standard for multi-regions in the documentation but TDR
    # returns 'us' in lowercase
    require(source.location.lower() == config.tdr_source_location.lower(),
            'Actual storage location of TDR source differs from configured one',
            source.location, config.tdr_source_location)
    # FIXME: Eliminate azul.terra.TDRClient.TDRSource
    #        https://github.com/DataBiosphere/azul/issues/5524
    ref = TDRSourceRef(id=source.id, spec=source_spec)
    plugin = client.repository_plugin(catalog)
    subgraph_count = sum(plugin.list_partitions(ref).values())
    require(subgraph_count > 0,
            'Source spec is empty (bad prefix?)', source_spec)


def verify_source_access():
    public_snapshots = public_tdr.snapshot_ids()
    all_snapshots = tdr.snapshot_ids()
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
