import logging
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
)
from azul.types import (
    JSON,
    MutableJSONs,
)

log = logging.getLogger(__name__)


class Plugin(RepositoryPlugin):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.client = AzulTDRClient(config.tdr_bigquery_dataset)

    def list_bundles(self, prefix: str) -> List[BundleFQID]:
        log.info('Listing bundles in prefix %s.', prefix)
        bundle_ids = self.client.list_links_ids(prefix)
        log.info('Prefix %s contains %i bundle(s).', prefix, len(bundle_ids))
        return bundle_ids

    @deprecated
    def fetch_bundle_manifest(self, bundle_fqid: BundleFQID) -> MutableJSONs:
        raise NotImplementedError

    def fetch_bundle(self, bundle_fqid: BundleFQID) -> Bundle:
        raise NotImplementedError

    def portal_db(self) -> Sequence[JSON]:
        return []

    def dss_deletion_subscription_query(self, prefix: str) -> JSON:
        raise NotImplementedError

    def dss_subscription_query(self, prefix: str) -> JSON:
        raise NotImplementedError
