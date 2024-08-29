from concurrent.futures import (
    ThreadPoolExecutor,
    as_completed,
)
import logging
from typing import (
    Collection,
)

from azul import (
    CatalogName,
    cache_per_thread,
    cached_property,
    config,
    require,
)
from azul.azulclient import (
    AzulClient,
)
from azul.logging import (
    configure_script_logging,
)
from azul.plugins.repository.tdr import (
    TDRPlugin,
)
from azul.terra import (
    TDRClient,
    TDRSourceSpec,
)

log = logging.getLogger(__name__)


class TerraValidator:

    @cached_property
    def azul_client(self) -> AzulClient:
        return AzulClient()

    @property
    @cache_per_thread
    def tdr(self) -> TDRClient:
        return TDRClient.for_indexer()

    @property
    @cache_per_thread
    def public_tdr(self) -> TDRClient:
        return TDRClient.for_anonymous_user()

    def repository_plugin(self, catalog: CatalogName) -> TDRPlugin:
        assert catalog in self.catalogs, catalog
        plugin = self.azul_client.repository_plugin(catalog)
        assert isinstance(plugin, TDRPlugin), plugin
        return plugin

    @cached_property
    def catalogs(self) -> Collection[CatalogName]:
        result = [
            catalog.name
            for catalog in config.catalogs.values()
            if (
                config.is_tdr_enabled(catalog.name)
                and catalog.name not in config.integration_test_catalogs
            )
        ]
        assert result, config.catalogs
        return result

    def register_with_sam(self) -> None:
        for tdr in self.tdr, self.public_tdr:
            tdr.register_with_sam()
            require(tdr.is_registered())

    def verify_sources(self) -> None:
        futures = []
        all_sources = set()
        with ThreadPoolExecutor(max_workers=8) as tpe:
            for catalog in self.catalogs:
                catalog_sources = config.sources(catalog)
                for source in catalog_sources - all_sources:
                    source = TDRSourceSpec.parse(source)
                    futures.append(tpe.submit(self.verify_source, catalog, source))
                all_sources |= catalog_sources
            for completed_future in as_completed(futures):
                futures.remove(completed_future)
                e = completed_future.exception()
                if e is not None:
                    for running_future in futures:
                        running_future.cancel()
                    raise e

    def verify_source(self,
                      catalog: CatalogName,
                      source_spec: TDRSourceSpec
                      ) -> None:
        plugin = self.repository_plugin(catalog)
        ref = plugin.resolve_source(str(source_spec))
        log.info('TDR client is authorized for API access to %s.', source_spec)
        ref = plugin.partition_source(catalog, ref)
        prefix = ref.spec.prefix
        if config.deployment.is_main:
            require(prefix.common == '', source_spec)
            self.tdr.check_bigquery_access(source_spec)
        else:
            subgraph_count = len(plugin.list_bundles(ref, prefix.common))
            require(subgraph_count > 0, 'Common prefix is too long', ref.spec)
            require(subgraph_count <= 512, 'Common prefix is too short', ref.spec)

    def verify_source_access(self) -> None:
        public_snapshots = self.public_tdr.snapshot_ids()
        all_snapshots = self.tdr.snapshot_ids()
        diff = public_snapshots - all_snapshots
        require(not diff,
                'The public service account can access snapshots that the indexer '
                'service account cannot', diff)


def main():
    configure_script_logging(log)
    validator = TerraValidator()
    validator.register_with_sam()
    validator.verify_sources()
    validator.verify_source_access()


if __name__ == '__main__':
    main()
