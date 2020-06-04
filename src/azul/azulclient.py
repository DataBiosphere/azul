from collections import defaultdict
from concurrent.futures import (
    Future,
    ThreadPoolExecutor,
)
from functools import (
    cached_property,
    partial,
)
from itertools import (
    groupby,
    product,
)
import json
import logging
from pprint import PrettyPrinter
from typing import (
    Iterable,
    List,
)
from urllib.parse import (
    urlparse,
)
import uuid

from boltons.cacheutils import cachedproperty
from more_itertools import chunked
import requests

from azul import (
    config,
    hmac,
)
from azul.indexer import BundleFQID
from azul.indexer.index_service import IndexService
from azul.plugins import (
    RepositoryPlugin,
)

logger = logging.getLogger(__name__)


class AzulClient(object):

    def __init__(self,
                 prefix: str = config.dss_query_prefix,
                 num_workers: int = 16):
        self.num_workers = num_workers
        self.prefix = prefix

    @cachedproperty
    def repository_plugin(self) -> RepositoryPlugin:
        return RepositoryPlugin.load().create()

    @cachedproperty
    def query(self):
        return self.repository_plugin.dss_subscription_query(self.prefix)

    def post_bundle(self, indexer_url, notification):
        """
        Send a mock DSS notification to the indexer
        """
        response = requests.post(indexer_url, json=notification, auth=hmac.prepare())
        response.raise_for_status()
        return response.content

    def synthesize_notification(self, bundle_fqid: BundleFQID, **payload: str):
        """
        Generate a indexer notification for the given bundle.

        The returned notification is considered synthetic in contrast to the
        organic ones sent by DSS. They can be easily identified by the special
        subscription UUID.
        """
        bundle_uuid, bundle_version = bundle_fqid
        return {
            "query": self.query,
            "subscription_id": "cafebabe-feed-4bad-dead-beaf8badf00d",
            "transaction_id": str(uuid.uuid4()),
            "match": {
                "bundle_uuid": bundle_uuid,
                "bundle_version": bundle_version
            },
            **payload
        }

    def reindex(self):
        bundle_fqids = self.list_bundles()
        notifications = [self.synthesize_notification(fqid) for fqid in bundle_fqids]
        self._index(notifications)

    def bundle_has_project_json(self, bundle_fqid: BundleFQID) -> bool:
        manifest = self.repository_plugin.fetch_bundle_manifest(bundle_fqid)
        # Since we now use DSS' GET /bundles/all which doesn't support filtering, we need to filter by hand
        return any(f['name'] == 'project_0.json' and f['indexed'] for f in manifest['bundle']['files'])

    def _index(self, notifications: Iterable, path: str = '/'):
        errors = defaultdict(int)
        missing = []
        indexed = 0
        total = 0
        indexer_url = config.indexer_endpoint() + path

        with ThreadPoolExecutor(max_workers=self.num_workers, thread_name_prefix='pool') as tpe:

            def attempt(notification, i):
                try:
                    logger.info("Sending notification %s to %s -- attempt %i:", notification, indexer_url, i)
                    url = urlparse(indexer_url)
                    self.post_bundle(url.geturl(), notification)
                except requests.HTTPError as e:
                    if i < 3:
                        logger.warning("Notification %s, attempt %i: retrying after error %s", notification, i, e)
                        return notification, tpe.submit(partial(attempt, notification, i + 1))
                    else:
                        logger.warning("Notification %s, attempt %i: giving up after error %s", notification, i, e)
                        return notification, e
                else:
                    logger.info("Notification %s, attempt %i: success", notification, i)
                    return notification, None

            def handle_future(future):
                # @formatter:off
                nonlocal indexed
                # @formatter:on
                # Block until future raises or succeeds
                exception = future.exception()
                if exception is None:
                    bundle_fqid, result = future.result()
                    if result is None:
                        indexed += 1
                    elif isinstance(result, requests.HTTPError):
                        status_code = result.response.status_code
                        errors[status_code] += 1
                        missing.append((notification, status_code))
                    elif isinstance(result, Future):
                        # The task scheduled a follow-on task, presumably a retry. Follow that new task.
                        handle_future(result)
                    else:
                        assert False
                else:
                    logger.warning("Unhandled exception in worker:", exc_info=exception)

            futures = []
            for notification in notifications:
                total += 1
                futures.append(tpe.submit(partial(attempt, notification, 0)))
            for future in futures:
                handle_future(future)

        printer = PrettyPrinter(stream=None, indent=1, width=80, depth=None, compact=False)
        logger.info("Total of bundle FQIDs read: %i", total)
        logger.info("Total of bundle FQIDs indexed: %i", indexed)
        logger.error("Total number of errors by code:\n%s", printer.pformat(dict(errors)))
        logger.error("Missing bundle_fqids and their error code:\n%s", printer.pformat(missing))
        if errors or missing:
            raise AzulClientNotificationError()

    def list_bundles(self) -> List[BundleFQID]:
        return self.repository_plugin.list_bundles(self.prefix)

    @cachedproperty
    def sqs(self):
        import boto3
        return boto3.resource('sqs')

    @cachedproperty
    def notifications_queue(self):
        return self.sqs.get_queue_by_name(QueueName=config.notifications_queue_name())

    def remote_reindex(self, partition_prefix_length):
        partition_prefixes = map(''.join, product('0123456789abcdef', repeat=partition_prefix_length))

        def message(partition_prefix):
            prefix = self.prefix + partition_prefix
            logger.info('Preparing message for partition with prefix %s', prefix)
            return dict(action='reindex',
                        dss_url=config.dss_endpoint,
                        prefix=prefix)

        messages = map(message, partition_prefixes)
        for batch in chunked(messages, 10):
            entries = [
                dict(Id=str(i), MessageBody=json.dumps(message))
                for i, message in enumerate(batch)
            ]
            self.notifications_queue.send_messages(Entries=entries)

    @classmethod
    def do_remote_reindex(cls, message):
        assert message['dss_url'] == config.dss_endpoint
        self = cls(prefix=message['prefix'])
        bundle_fqids = self.list_bundles()
        bundle_fqids = cls._filter_obsolete_bundle_versions(bundle_fqids)
        logger.info("After filtering obsolete versions, %i bundles remain in prefix %s",
                    len(bundle_fqids), self.prefix)
        messages = (dict(action='add', notification=self.synthesize_notification(bundle_fqid))
                    for bundle_fqid in bundle_fqids)
        num_messages = 0
        for batch in chunked(messages, 10):
            entries = [
                dict(Id=str(i), MessageBody=json.dumps(message))
                for i, message in enumerate(batch)
            ]
            self.notifications_queue.send_messages(Entries=entries)
            num_messages += len(batch)
        logger.info('Successfully queued %i notification(s) for prefix %s', num_messages, self.prefix)

    @classmethod
    def _filter_obsolete_bundle_versions(cls, bundle_fqids: Iterable[BundleFQID]) -> List[BundleFQID]:
        # noinspection PyProtectedMember
        """
        Suppress obsolete bundle versions by only taking the latest version for each bundle UUID.

        >>> AzulClient._filter_obsolete_bundle_versions([])
        []

        >>> B = BundleFQID
        >>> AzulClient._filter_obsolete_bundle_versions([B('c', '0'), B('a', '1'), B('b', '3')])
        [BundleFQID(uuid='c', version='0'), BundleFQID(uuid='b', version='3'), BundleFQID(uuid='a', version='1')]

        >>> AzulClient._filter_obsolete_bundle_versions([B('C', '0'), B('a', '1'), B('a', '0'), \
                                                         B('a', '2'), B('b', '1'), B('c', '2')])
        [BundleFQID(uuid='c', version='2'), BundleFQID(uuid='b', version='1'), BundleFQID(uuid='a', version='2')]

        >>> AzulClient._filter_obsolete_bundle_versions([B('a', '0'), B('A', '1')])
        [BundleFQID(uuid='A', version='1')]
        """
        # Sort lexicographically by FQID. I've observed the DSS response to
        # already be in this order
        bundle_fqids = sorted(bundle_fqids,
                              key=lambda fqid: (fqid[0].lower(), fqid[1].lower()),
                              reverse=True)
        # Group by bundle UUID
        bundle_fqids = groupby(bundle_fqids, key=lambda fqid: fqid[0].lower())
        # Take the first item in each group. Because the oder is reversed, this
        # is the latest version
        bundle_fqids = [next(group) for _, group in bundle_fqids]
        return bundle_fqids

    @cached_property
    def index_service(self):
        return IndexService()

    def delete_all_indices(self):
        self.index_service.delete_indices()

    def create_all_indices(self):
        self.index_service.create_indices()

    def delete_bundle(self, bundle_uuid, bundle_version):
        logger.info('Deleting bundle %s.%s', bundle_uuid, bundle_version)
        notification = [
            {
                'match': {
                    'bundle_uuid': bundle_uuid,
                    'bundle_version': bundle_version
                }
            }
        ]
        self.delete_notification(notification)

    def delete_notification(self, notifications):
        self._index(notifications, path='/delete')


class AzulClientError(RuntimeError):
    pass


class AzulClientNotificationError(AzulClientError):

    def __init__(self) -> None:
        super().__init__('Some notifications could not be sent')
