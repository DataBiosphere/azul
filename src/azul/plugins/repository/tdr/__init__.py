from functools import cached_property
import logging
import time
from typing import (
    List,
    Sequence,
)

from deprecated import deprecated

from azul import config
from azul.indexer import (
    Bundle,
    BundleFQID,
)
from azul.plugins import (
    RepositoryPlugin,
)
from azul.tdr import (
    AzulTDRClient,
    BigQueryDataset,
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

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @cached_property
    def client(self):
        import azul.dss
        with azul.dss.shared_credentials():
            return AzulTDRClient(dataset=BigQueryDataset.parse(config.tdr_target))

    def list_bundles(self, prefix: str) -> List[BundleFQID]:
        log.info('Listing bundles in prefix %s.', prefix)
        bundle_ids = self.client.list_links_ids(prefix)
        log.info('Prefix %s contains %i bundle(s).', prefix, len(bundle_ids))
        return bundle_ids

    @deprecated
    def fetch_bundle_manifest(self, bundle_fqid: BundleFQID) -> MutableJSONs:
        raise NotImplementedError()

    def fetch_bundle(self, bundle_fqid: BundleFQID) -> Bundle:
        now = time.time()
        bundle = self.client.emulate_bundle(bundle_fqid)
        log.info("It took %.003fs to download bundle %s.%s",
                 time.time() - now, bundle.uuid, bundle.version)
        return bundle

    def portal_db(self) -> Sequence[JSON]:
        return []

    def dss_deletion_subscription_query(self, prefix: str) -> JSON:
        return {}

    def dss_subscription_query(self, prefix: str) -> JSON:
        return {}
