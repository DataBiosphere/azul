import json
import logging
import uuid
from collections import defaultdict
from concurrent.futures import (
    Future,
    ThreadPoolExecutor,
)
from functools import (
    partial,
    lru_cache,
)
from itertools import (
    product,
    groupby,
)
from pprint import PrettyPrinter
from typing import (
    List,
    Iterable,
    Tuple,
)
from urllib.parse import (
    urlparse,
)

import requests
from more_itertools import chunked

import azul.dss
from azul import (
    config,
    hmac,
)
from azul.es import ESClientFactory
from azul.plugin import Plugin

logger = logging.getLogger(__name__)

FQID = Tuple[str, str]


class AzulClient(object):

    def __init__(self,
                 indexer_url: str = config.indexer_endpoint(),
                 dss_url: str = config.dss_endpoint,
                 prefix: str = config.dss_query_prefix,
                 num_workers: int = 16):
        self.num_workers = num_workers
        self.prefix = prefix
        self.dss_url = dss_url
        self.indexer_url = indexer_url

    @lru_cache()
    def query(self):
        return Plugin.load().dss_subscription_query(self.prefix)

    def post_bundle(self, indexer_url, notification):
        """
        Send a mock DSS notification to the indexer
        """
        response = requests.post(indexer_url, json=notification, auth=hmac.prepare())
        response.raise_for_status()
        return response.content

    def synthesize_notification(self, bundle_fqid: FQID, **payload):
        """
        Generate a indexer notification for the given bundle.

        The returned notification is considered synthetic in contrast to the
        organic ones sent by DSS. They can be easily identified by the special
        subscription UUID.
        """
        bundle_uuid, bundle_version = bundle_fqid
        return {
            "query": self.query(),
            "subscription_id": "cafebabe-feed-4bad-dead-beaf8badf00d",
            "transaction_id": str(uuid.uuid4()),
            "match": {
                "bundle_uuid": bundle_uuid,
                "bundle_version": bundle_version
            },
            **payload
        }

    def reindex(self):
        bundle_fqids = self.list_dss_bundles()
        notifications = [self.synthesize_notification(fqid) for fqid in bundle_fqids]
        self._index(notifications)

    def bundle_has_project_json(self, bundle_uuid, bundle_version):
        manifest = self.dss_client.get_bundle(uuid=bundle_uuid, version=bundle_version, replica='aws')
        # Since we now use DSS' GET /bundles/all which doesn't support filtering, we need to filter by hand
        # FIXME: handle bundles with more than 500 files where project_0.json is not on first page of manifest
        return any(f['name'] == 'project_0.json' and f['indexed'] for f in manifest['bundle']['files'])

    def _index(self, notifications: Iterable, path: str = '/'):
        errors = defaultdict(int)
        missing = []
        indexed = 0
        total = 0
        indexer_url = self.indexer_url + path

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
                        errors[result.code] += 1
                        missing.append((notification, result.code))
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
        logger.warning("Total number of errors by code:\n%s", printer.pformat(dict(errors)))
        logger.warning("Missing bundle_fqids and their error code:\n%s", printer.pformat(missing))

    def list_dss_bundles(self) -> List[FQID]:
        logger.info('Listing bundles in prefix %s.', self.prefix)
        bundle_fqids = []
        # FIXME: get_bundles_all.iterate returns empty result due to bug in hca 6.4.0
        #        Fixed by https://github.com/HumanCellAtlas/dcp-cli/commit/b963d284957a6ccedf16d61278f04b151ff2f46c
        #        Switch to get_bundles_all.iterate() when upgrading to 6.5.1 or later
        #        https://github.com/DataBiosphere/azul/issues/1486
        response = self.dss_client.get_bundles_all.paginate(prefix=self.prefix, replica='aws', per_page=500)
        for page in response:
            bundle_fqids.extend((bundle['uuid'], bundle['version']) for bundle in page['bundles'])
        logger.info('Prefix %s contains %i bundle(s).', self.prefix, len(bundle_fqids))
        return bundle_fqids

    @property
    @lru_cache(maxsize=1)
    def dss_client(self):
        return azul.dss.client(dss_endpoint=self.dss_url)

    @property
    @lru_cache(maxsize=1)
    def sqs(self):
        import boto3
        return boto3.resource('sqs')

    @lru_cache(maxsize=10)
    def queue(self, queue_name):
        return self.sqs.get_queue_by_name(QueueName=queue_name)

    def remote_reindex(self, partition_prefix_length):
        partition_prefixes = map(''.join, product('0123456789abcdef', repeat=partition_prefix_length))

        def message(partition_prefix):
            prefix = self.prefix + partition_prefix
            logger.info('Preparing message for partition with prefix %s', prefix)
            return dict(action='reindex',
                        dss_url=self.dss_url,
                        prefix=prefix)

        notify_queue = self.queue(config.notify_queue_name)
        messages = map(message, partition_prefixes)
        for batch in chunked(messages, 10):
            notify_queue.send_messages(Entries=[dict(Id=str(i), MessageBody=json.dumps(message))
                                                for i, message in enumerate(batch)])

    @classmethod
    def do_remote_reindex(cls, message):
        self = cls(dss_url=message['dss_url'],
                   prefix=message['prefix'])
        bundle_fqids = self.list_dss_bundles()
        bundle_fqids = cls._filter_obsolete_bundle_versions(bundle_fqids)
        logger.info("After filtering obsolete versions, %i bundles remain in prefix %s",
                    len(bundle_fqids), self.prefix)
        messages = (dict(action='add', notification=self.synthesize_notification(bundle_fqid))
                    for bundle_fqid in bundle_fqids)
        notify_queue = self.queue(config.notify_queue_name)
        num_messages = 0
        for batch in chunked(messages, 10):
            notify_queue.send_messages(Entries=[dict(Id=str(i), MessageBody=json.dumps(message))
                                                for i, message in enumerate(batch)])
            num_messages += len(batch)
        logger.info('Successfully queued %i notification(s) for prefix %s', num_messages, self.prefix)

    @classmethod
    def _filter_obsolete_bundle_versions(cls, bundle_fqids: Iterable[FQID]) -> List[FQID]:
        # noinspection PyProtectedMember
        """
        Suppress obsolete bundle versions by only taking the latest version for each bundle UUID.

        >>> AzulClient._filter_obsolete_bundle_versions([])
        []

        >>> AzulClient._filter_obsolete_bundle_versions([('c', '0'), ('a', '1'), ('b', '3')])
        [('c', '0'), ('b', '3'), ('a', '1')]

        >>> AzulClient._filter_obsolete_bundle_versions([('C', '0'), ('a', '1'), ('a', '0'), \
                                                         ('a', '2'), ('b', '1'), ('c', '2')])
        [('c', '2'), ('b', '1'), ('a', '2')]

        >>> AzulClient._filter_obsolete_bundle_versions([('a', '0'), ('A', '1')])
        [('A', '1')]
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

    def delete_all_indices(self):
        es_client = ESClientFactory.get()
        plugin = Plugin.load()
        indexer_cls = plugin.indexer_class()
        indexer = indexer_cls()
        for index_name in indexer.index_names():
            if es_client.indices.exists(index_name):
                es_client.indices.delete(index=index_name)

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
