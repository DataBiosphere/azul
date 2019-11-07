import random
import sys
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
import json
import logging
from pprint import PrettyPrinter

import requests
from typing import (
    List,
    Iterable,
    Set,
    Tuple,
)
from urllib.error import HTTPError
from urllib.parse import (
    urlparse,
)

from more_itertools import chunked

from azul import (
    config,
    hmac,
)
import azul.dss
from azul.es import ESClientFactory
from azul.plugin import Plugin
from azul.types import JSON

logger = logging.getLogger(__name__)

FQID = Tuple[str, str]


class AzulClient(object):

    def __init__(self,
                 indexer_url: str = config.indexer_endpoint(),
                 dss_url: str = config.dss_endpoint,
                 prefix: str = config.dss_query_prefix,
                 num_workers: int = 16,
                 dryrun: bool = False):
        self.num_workers = num_workers
        self.prefix = prefix
        self.dss_url = dss_url
        self.indexer_url = indexer_url
        self.dryrun = dryrun

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

    def _make_notification(self, bundle_fqid: FQID):
        bundle_uuid, bundle_version = bundle_fqid
        return {
            "query": self.query(),
            "subscription_id": 'cafebabe-feed-4bad-dead-beaf8badf00d',
            "transaction_id": str(uuid.uuid4()),
            "match": {
                "bundle_uuid": bundle_uuid,
                "bundle_version": bundle_version
            },
        }

    def reindex(self):
        bundle_fqids = self._list_dss_bundles()
        notifications = [self._make_notification(fqid) for fqid in bundle_fqids]
        self._index(notifications)

    def test_notifications(self, test_name: str, test_uuid: str, max_bundles: int) -> Tuple[List[JSON], Set[FQID]]:
        bundle_fqids = self._list_dss_bundles()
        bundle_fqids = self._prune_test_bundles(bundle_fqids, max_bundles)
        notifications = []
        test_bundle_fqids = set()
        for bundle_fqid in bundle_fqids:
            new_bundle_uuid = str(uuid.uuid4())
            # Using a new version helps ensure that any aggregation will choose
            # the contribution from the test bundle over that by any other
            # bundle not in the test set.
            # Failing to do this before caused https://github.com/DataBiosphere/azul/issues/1174
            new_bundle_version = azul.dss.new_version()
            test_bundle_fqids.add((new_bundle_uuid, new_bundle_version))
            notification = dict(self._make_notification(bundle_fqid),
                                test_bundle_uuid=new_bundle_uuid,
                                test_bundle_version=new_bundle_version,
                                test_name=test_name,
                                test_uuid=test_uuid)
            notifications.append(notification)
        return notifications, test_bundle_fqids

    def _prune_test_bundles(self, bundle_fqids, max_bundles):
        filtered_bundle_fqids = []
        seed = random.randint(0, sys.maxsize)
        logger.info('Selecting %i out of %i candidate bundle(s) with random seed %i.',
                    max_bundles, len(bundle_fqids), seed)
        random_ = random.Random(x=seed)
        bundle_fqids = random_.sample(bundle_fqids, len(bundle_fqids))
        for bundle_uuid, bundle_version in bundle_fqids:
            if len(filtered_bundle_fqids) < max_bundles:
                manifest = self.dss_client.get_bundle(uuid=bundle_uuid, version=bundle_version, replica='aws')
                # Since we now use DSS' GET /bundles/all which doesn't support filtering, we need to filter by hand
                # FIXME: handle bundles with more than 500 files where project_0.json is not on first page of manifest
                if any(f['name'] == 'project_0.json' and f['indexed'] for f in manifest['bundle']['files']):
                    filtered_bundle_fqids.append((bundle_uuid, bundle_version))
            else:
                break
        return filtered_bundle_fqids

    def _index(self, notifications: Iterable, path: str = '/'):
        errors = defaultdict(int)
        missing = []
        indexed = 0
        total = 0
        indexer_url = self.indexer_url + path

        with ThreadPoolExecutor(max_workers=self.num_workers, thread_name_prefix='pool') as tpe:

            def attempt(notification, i):
                try:
                    logger.info("Sending notification %s -- attempt %i:", notification, i)
                    url = urlparse(indexer_url)
                    if not self.dryrun:
                        self.post_bundle(url.geturl(), notification)
                except HTTPError as e:
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
                    elif isinstance(result, HTTPError):
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

    def _list_dss_bundles(self) -> List[FQID]:
        logger.info('Listing bundles in prefix %s.', self.prefix)
        bundle_fqids = []
        request = dict(prefix=self.prefix, replica='aws', per_page=500)
        while True:
            response = self.dss_client.get_bundles_all(**request)
            bundle_fqids.extend((bundle['uuid'], bundle['version']) for bundle in response['bundles'])
            if response['has_more']:
                request['token'] = response['token']
            else:
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
                        dryrun=self.dryrun,
                        prefix=prefix)

        notify_queue = self.queue(config.notify_queue_name)
        messages = map(message, partition_prefixes)
        for batch in chunked(messages, 10):
            notify_queue.send_messages(Entries=[dict(Id=str(i), MessageBody=json.dumps(message))
                                                for i, message in enumerate(batch)])

    @classmethod
    def do_remote_reindex(cls, message):
        self = cls(dss_url=message['dss_url'],
                   prefix=message['prefix'],
                   dryrun=message['dryrun'])
        bundle_fqids = self._list_dss_bundles()
        bundle_fqids = cls._filter_obsolete_bundle_versions(bundle_fqids)
        logger.info("After filtering obsolete versions, %i bundles remain in prefix %s",
                    len(bundle_fqids), self.prefix)
        messages = (dict(action='add', notification=self._make_notification(bundle_fqid))
                    for bundle_fqid in bundle_fqids)
        notify_queue = self.queue(config.notify_queue_name)
        num_messages = 0
        for batch in chunked(messages, 10):
            if not self.dryrun:
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
                if self.dryrun:
                    logger.info("Would delete index '%s'", index_name)
                else:
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
