from collections import defaultdict
from concurrent.futures import (
    Future,
    ThreadPoolExecutor,
)
from functools import (
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
import uuid

from furl import furl
from more_itertools import chunked
import requests

from azul import (
    CatalogName,
    cached_property,
    config,
    hmac,
)
from azul.indexer import BundleFQID
from azul.indexer.index_service import IndexService
from azul.plugins import (
    RepositoryPlugin,
)
from azul.queues import Queues
from azul.types import JSON
from azul.uuids import validate_uuid_prefix

logger = logging.getLogger(__name__)


class AzulClient(object):

    def __init__(self,
                 prefix: str = config.dss_query_prefix,
                 num_workers: int = 16):
        validate_uuid_prefix(prefix)
        self.num_workers = num_workers
        self.prefix = prefix

    @cached_property
    def repository_plugin(self) -> RepositoryPlugin:
        return RepositoryPlugin.load().create()

    @cached_property
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

    def reindex(self, catalog: CatalogName):
        bundle_fqids = self.list_bundles()
        notifications = [self.synthesize_notification(fqid) for fqid in bundle_fqids]
        self.index(catalog, notifications)

    def bundle_has_project_json(self, bundle_fqid: BundleFQID) -> bool:
        try:
            manifest = self.repository_plugin.fetch_bundle_manifest(bundle_fqid)
        except NotImplementedError:
            # If the plugin doesn't support the method we'll just assume that
            # every bundle references a project.
            return True
        else:
            # Since we now use DSS' GET /bundles/all which doesn't support
            # filtering, we need to filter by hand.
            return any(f['name'] == 'project_0.json' and f['indexed'] for f in manifest)

    def index(self, catalog: CatalogName, notifications: Iterable, delete: bool = False):
        errors = defaultdict(int)
        missing = []
        indexed = 0
        total = 0
        indexer_url = furl(url=config.indexer_endpoint(),
                           path=(catalog, 'delete' if delete else 'add'))

        with ThreadPoolExecutor(max_workers=self.num_workers, thread_name_prefix='pool') as tpe:

            def attempt(notification, i):
                log_args = (indexer_url.url, notification, i)
                try:
                    logger.info("Notifying %s about %s, attempt %i.", *log_args)
                    self.post_bundle(indexer_url.url, notification)
                except (requests.HTTPError, requests.ConnectionError) as e:
                    if i < 3:
                        logger.warning("Retrying to notify %s about %s, attempt %i, after error %s.", *log_args, e)
                        return notification, tpe.submit(partial(attempt, notification, i + 1))
                    else:
                        logger.warning("Failed to notify %s about %s, attempt %i: after error %s.", *log_args, e)
                        return notification, e
                else:
                    logger.info("Success notifying %s about %s, attempt %i.", *log_args)
                    return notification, None

            def handle_future(future):
                # @formatter:off
                nonlocal indexed
                # @formatter:on
                bundle_fqid, result = future.result()
                if result is None:
                    indexed += 1
                elif isinstance(result, (requests.HTTPError, requests.ConnectionError)):
                    status_code = result.response.status_code
                    errors[status_code] += 1
                    missing.append((notification, status_code))
                elif isinstance(result, Future):
                    # The task scheduled a follow-on task, presumably a retry. Follow that new task.
                    handle_future(result)
                else:
                    assert False

            futures = []
            for notification in notifications:
                total += 1
                futures.append(tpe.submit(partial(attempt, notification, 0)))
            for future in futures:
                handle_future(future)

        printer = PrettyPrinter(stream=None, indent=1, width=80, depth=None, compact=False)
        logger.info("Total of bundle FQIDs read: %i", total)
        logger.info("Total of bundle FQIDs indexed: %i", indexed)
        if errors:
            logger.error("Total number of errors by HTTP status code:\n%s",
                         printer.pformat(dict(errors)))
        if missing:
            logger.error("Unsent notifications and their HTTP status code:\n%s",
                         printer.pformat(missing))
        if errors or missing:
            raise AzulClientNotificationError()

    def list_bundles(self) -> List[BundleFQID]:
        return self.repository_plugin.list_bundles(self.prefix)

    @cached_property
    def sqs(self):
        import boto3
        return boto3.resource('sqs')

    @cached_property
    def notifications_queue(self):
        return self.sqs.get_queue_by_name(QueueName=config.notifications_queue_name())

    def remote_reindex(self, catalog: CatalogName, partition_prefix_length):
        partition_prefixes = map(''.join, product('0123456789abcdef', repeat=partition_prefix_length))

        def message(partition_prefix):
            prefix = self.prefix + partition_prefix
            logger.info('Preparing message for partition with prefix %s', prefix)
            return dict(action='reindex',
                        catalog=catalog,
                        source=self.repository_plugin.source,
                        prefix=prefix)

        messages = map(message, partition_prefixes)
        for batch in chunked(messages, 10):
            entries = [
                dict(Id=str(i), MessageBody=json.dumps(message))
                for i, message in enumerate(batch)
            ]
            self.notifications_queue.send_messages(Entries=entries)

    @classmethod
    def do_remote_reindex(cls, message: JSON) -> None:
        self = cls(prefix=message['prefix'])
        self._do_remote_index(message)

    def _do_remote_index(self, message: JSON) -> None:
        assert message['source'] == self.repository_plugin.source
        catalog = message['catalog']
        bundle_fqids = self.list_bundles()
        bundle_fqids = self._filter_obsolete_bundle_versions(bundle_fqids)
        logger.info('After filtering obsolete versions, %i bundles remain in prefix %s',
                    len(bundle_fqids), self.prefix)
        messages = (
            {
                'action': 'add',
                'notification': self.synthesize_notification(bundle_fqid),
                'catalog': catalog
            }
            for bundle_fqid in bundle_fqids
        )
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

    def delete_all_indices(self, catalog: CatalogName):
        self.index_service.delete_indices(catalog)

    def create_all_indices(self, catalog: CatalogName):
        self.index_service.create_indices(catalog)

    def delete_bundle(self, catalog: CatalogName, bundle_uuid, bundle_version):
        logger.info('Deleting bundle %s.%s', bundle_uuid, bundle_version)
        notifications = [
            {
                'match': {
                    'bundle_uuid': bundle_uuid,
                    'bundle_version': bundle_version
                }
            }
        ]
        self.index(catalog, notifications, delete=True)

    @cached_property
    def queues(self):
        return Queues()

    def reset_indexer(self,
                      catalog: CatalogName,
                      *,
                      purge_queues: bool,
                      delete_indices: bool,
                      create_indices: bool):
        """
        Reset the indexer, to a degree.

        :param catalog: the catalog for which to create or delete indices

        :param purge_queues: whether to purge the indexer queues at the
                             beginning. Note that purging the queues affects
                             all catalogs, not just the specified one.

        :param delete_indices: whether to delete the indexes before optionally
                               recreating them

        :param create_indices: whether to create the indexes at the end.
        """
        work_queues = self.queues.get_queues(config.work_queue_names)
        if purge_queues:
            logger.info('Disabling lambdas ...')
            self.queues.manage_lambdas(work_queues, enable=False)
            logger.info('Purging queues: %s', ', '.join(work_queues.keys()))
            self.queues.purge_queues_unsafely(work_queues)
        if delete_indices:
            logger.info('Deleting indices ...')
            self.delete_all_indices(catalog)
        if purge_queues:
            logger.info('Re-enabling lambdas ...')
            self.queues.manage_lambdas(work_queues, enable=True)
        if create_indices:
            logger.info('Creating indices ...')
            self.create_all_indices(catalog)

    def wait_for_indexer(self, **kwargs):
        """
        Wait for indexer to begin processing notifications, then wait for work
        to finish.

        :param kwargs: keyword arguments to Queues.wait_for_queue_level when
                       waiting for work to finish.
        """
        self.queues.wait_for_queue_level(empty=False)
        self.queues.wait_for_queue_level(empty=True, **kwargs)


class AzulClientError(RuntimeError):
    pass


class AzulClientNotificationError(AzulClientError):

    def __init__(self) -> None:
        super().__init__('Some notifications could not be sent')
