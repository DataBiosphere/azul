import logging
import time
from typing import (
    List,
    Sequence,
)

from deprecated import (
    deprecated,
)
from furl import (
    furl,
)

from azul import (
    cached_property,
    config,
)
from azul.indexer import (
    Bundle,
    BundleFQID,
)
from azul.plugins import (
    RepositoryPlugin,
)
from azul.tdr import (
    BigQueryClient,
    BigQueryDataset,
    TDRClient,
)
from azul.types import (
    JSON,
    MutableJSONs,
)

log = logging.getLogger(__name__)


class Plugin(RepositoryPlugin):

    @property
    def source(self) -> str:
        return config.tdr_target

    @cached_property
    def target(self) -> BigQueryDataset:
        return BigQueryDataset.parse(self.source)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @cached_property
    def bq_client(self):
        return BigQueryClient(target=self.target)

    @cached_property
    def api_client(self):
        return TDRClient()

    def list_bundles(self, prefix: str) -> List[BundleFQID]:
        log.info('Listing bundles in prefix %s.', prefix)
        bundle_ids = self.bq_client.list_links_ids(prefix)
        log.info('Prefix %s contains %i bundle(s).', prefix, len(bundle_ids))
        return bundle_ids

    @deprecated
    def fetch_bundle_manifest(self, bundle_fqid: BundleFQID) -> MutableJSONs:
        raise NotImplementedError()

    def fetch_bundle(self, bundle_fqid: BundleFQID) -> Bundle:
        now = time.time()
        bundle = self.bq_client.emulate_bundle(bundle_fqid)
        log.info("It took %.003fs to download bundle %s.%s",
                 time.time() - now, bundle.uuid, bundle.version)
        self._stash_target_id(bundle.manifest)
        return bundle

    def portal_db(self) -> Sequence[JSON]:
        return []

    def dss_deletion_subscription_query(self, prefix: str) -> JSON:
        return {}

    def dss_subscription_query(self, prefix: str) -> JSON:
        return {}

    def drs_path(self, manifest_entry: JSON, metadata: JSON) -> str:
        return f'v1_{manifest_entry["target_id"]}_{manifest_entry["uuid"]}'

    def drs_netloc(self) -> str:
        return furl(config.tdr_service_url).netloc

    def _stash_target_id(self, manifest_entries: MutableJSONs):
        target_id = self.api_client.get_target_id(self.target)
        for entry in manifest_entries:
            entry['target_id'] = target_id
