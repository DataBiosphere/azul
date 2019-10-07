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
    Optional,
    Iterable,
    Set,
    Tuple,
)
from urllib.error import HTTPError
from urllib.parse import (
    parse_qs,
    urlencode,
    urlparse,
)

from more_itertools import chunked

from azul import (
    config,
    hmac,
)
from azul.es import ESClientFactory
from azul.plugin import Plugin
from azul.types import JSON

logger = logging.getLogger(__name__)


class AzulClient(object):

    def __init__(self,
                 indexer_url: str = config.indexer_endpoint(),
                 dss_url: str = config.dss_endpoint,
                 query: Optional[JSON] = None,
                 prefix: str = config.dss_query_prefix,
                 num_workers: int = 16,
                 dryrun: bool = False):
        assert query is None or not prefix, "Cannot mix `prefix` and `query`"
        self.num_workers = num_workers
        self._query = query
        self.prefix = prefix
        self.dss_url = dss_url
        self.indexer_url = indexer_url
        self.dryrun = dryrun

    def query(self, prefix=None):
        if prefix is None:
            prefix = self.prefix
        if self._query is None:
            plugin = Plugin.load()
            return plugin.dss_subscription_query(prefix)
        else:
            assert not prefix
            return self._query

    def post_bundle(self, indexer_url, notification):
        """
        Send a mock DSS notification to the indexer
        """
        response = requests.post(indexer_url, json=notification, auth=hmac.prepare())
        response.raise_for_status()
        return response.content

    def _make_notification(self, bundle_fqid):
        bundle_uuid, _, bundle_version = bundle_fqid.partition('.')
        return {
            "query": self.query(),
            "subscription_id": 'cafebabe-feed-4bad-dead-beaf8badf00d',
            "transaction_id": str(uuid.uuid4()),
            "match": {
                "bundle_uuid": bundle_uuid,
                "bundle_version": bundle_version
            },
        }

    def reindex(self, sync: bool = False):
        logger.info('Querying DSS using %s', json.dumps(self.query(), indent=4))
        bundle_fqids = self._post_dss_search()
        logger.info("Bundle FQIDs to index: %i", len(bundle_fqids))
        notifications = [self._make_notification(fqid) for fqid in bundle_fqids]
        self._index(notifications, sync=sync)

    def test_notifications(self, test_name: str, test_uuid: str) -> Tuple[List[dict], Set[str]]:
        logger.info('Querying DSS using %s', json.dumps(self.query(), indent=4))
        real_bundle_fqids = self._post_dss_search()
        logger.info("Bundle FQIDs to index: %i", len(real_bundle_fqids))
        notifications = []
        effective_bundle_fqids = set()
        for bundle_fqid in real_bundle_fqids:
            new_bundle_uuid = str(uuid.uuid4())
            _, _, version = bundle_fqid.partition('.')
            effective_bundle_fqids.add(new_bundle_uuid + '.' + version)
            notification = dict(self._make_notification(bundle_fqid),
                                test_bundle_uuid=new_bundle_uuid,
                                test_name=test_name,
                                test_uuid=test_uuid)
            notifications.append(notification)
        return notifications, effective_bundle_fqids

    def _index(self, notifications: Iterable, sync: bool = False):
        errors = defaultdict(int)
        missing = []
        indexed = 0
        total = 0

        with ThreadPoolExecutor(max_workers=self.num_workers, thread_name_prefix='pool') as tpe:

            def attempt(notification, i):
                try:
                    logger.info("Sending notification %s -- attempt %i:", notification, i)
                    url = urlparse(self.indexer_url)
                    if sync:
                        # noinspection PyProtectedMember
                        url = url._replace(query=urlencode({**parse_qs(url.query),
                                                            'sync': sync}, doseq=True))
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

    def _post_dss_search(self) -> List[str]:
        """
        Works around https://github.com/HumanCellAtlas/data-store/issues/1768
        """
        kwargs = dict(es_query=self.query(), replica='aws', per_page=500)
        bundle_fqids, url = [], None
        while True:
            # noinspection PyProtectedMember
            page = self.dss_client.post_search._request(kwargs, url=url)
            body = page.json()
            for result in body['results']:
                bundle_fqids.append(result['bundle_fqid'])
            try:
                next_link = page.links['next']
            except KeyError:
                total = body['total_hits']
                if len(bundle_fqids) == total:
                    return bundle_fqids
                else:
                    logger.warning('Result count mismatch: expected %i, actual %i. Restarting bundle listing.',
                                   total, len(bundle_fqids))
                    bundle_fqids, url = [], None
            else:
                url = next_link['url']

    @property
    @lru_cache(maxsize=1)
    def dss_client(self):
        return config.dss_client(dss_endpoint=self.dss_url)

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
            logger.debug('DSS query for partion is\n%s', json.dumps(self.query(), indent=4))
            return dict(action='reindex',
                        dss_url=self.dss_url,
                        query=self.query(prefix),
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
                   query=message['query'],
                   prefix='',
                   dryrun=message['dryrun'])
        prefix = message['prefix']
        # Render list of bundle FQIDs before moving on, reducing probability of duplicate notifications on retries
        bundle_fqids = self._post_dss_search()
        logger.info("DSS returned %i bundles for prefix %s", len(bundle_fqids), prefix)

        # FIXME: Make this more efficient by splitting the FQID into a tuple once.
        #        Right now, I favor a surgical change that cherry picks easily over one that's efficient.
        bundle_fqids = cls._filter_obsolete_bundle_versions(bundle_fqids)
        logger.info("After filtering obsolete versions, %i bundles remain for prefix %s", len(bundle_fqids), prefix)

        messages = (dict(action='add', notification=self._make_notification(bundle_fqid))
                    for bundle_fqid in bundle_fqids)
        notify_queue = self.queue(config.notify_queue_name)
        num_messages = 0
        for batch in chunked(messages, 10):
            if not self.dryrun:
                notify_queue.send_messages(Entries=[dict(Id=str(i), MessageBody=json.dumps(message))
                                                    for i, message in enumerate(batch)])
            num_messages += len(batch)
        logger.info('Successfully queued %i notification(s) for prefix %s', num_messages, prefix)

    @classmethod
    def _filter_obsolete_bundle_versions(cls, bundle_fqids: Iterable[str]) -> List[str]:
        """
        Suppress obsolete bundle versions by only taking the latest version for each bundle UUID.

        >>> AzulClient._filter_obsolete_bundle_versions([])
        []

        >>> AzulClient._filter_obsolete_bundle_versions(['c.0', 'a.1', 'b.3'])
        ['c.0', 'b.3', 'a.1']

        >>> AzulClient._filter_obsolete_bundle_versions(['C.0', 'a.1', 'a.0', 'a.2', 'b.1', 'c.2'])
        ['c.2', 'b.1', 'a.2']

        >>> AzulClient._filter_obsolete_bundle_versions(['a.0', 'A.1'])
        ['A.1']
        """
        # Sort lexicographically by FQID. I've observed the DSS response to already be in this order
        bundle_fqids = sorted(bundle_fqids, key=str.lower, reverse=True)
        # Group by bundle UUID
        bundle_fqids = groupby(bundle_fqids, key=lambda bundle_fqid: bundle_fqid.partition('.')[0].lower())
        # Take the first item in each group. Because the oder is reversed, this is the latest version
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
        notification = {
            'match': {
                'bundle_uuid': bundle_uuid,
                'bundle_version': bundle_version
            }
        }
        self.delete_notification(notification)

    def delete_notification(self, notification):
        response = requests.post(url=self.indexer_url + '/delete',
                                 json=notification,
                                 auth=hmac.prepare())
        response.raise_for_status()
