from collections import defaultdict
from concurrent.futures import Future, ThreadPoolExecutor
from copy import deepcopy
from functools import partial, lru_cache
from itertools import product
import json
import logging
from pprint import PrettyPrinter
from urllib.error import HTTPError
from urllib.parse import parse_qs, urlencode, urlparse
from urllib.request import Request, urlopen
from uuid import uuid4

from more_itertools import chunked

from azul import config
from azul.es import ESClientFactory
from azul.plugin import Plugin
from azul.types import JSON

logger = logging.getLogger(__name__)


class Reindexer(object):

    def __init__(self,
                 indexer_url: str = "https://" + config.api_lambda_domain('indexer') + "/",
                 dss_url: str = config.dss_endpoint,
                 es_query: JSON = None,
                 num_workers: int = 16,
                 dryrun: bool = False):

        if es_query is None:
            plugin = Plugin.load()
            es_query = plugin.dss_subscription_query()

        self.num_workers = num_workers
        self.es_query = es_query
        self.dss_url = dss_url
        self.indexer_url = indexer_url
        self.dryrun = dryrun

    def post_bundle(self, bundle_fqid, indexer_url):
        """
        Send a mock DSS notification to the indexer
        """
        bundle_uuid, _, bundle_version = bundle_fqid.partition('.')
        simulated_event = self._make_notification(bundle_uuid, bundle_version)
        body = json.dumps(simulated_event).encode('utf-8')
        request = Request(indexer_url, body)
        request.add_header("content-type", "application/json")
        with urlopen(request) as f:
            return f.read()

    def _make_notification(self, bundle_uuid, bundle_version):
        return {
            "query": self.es_query,
            "subscription_id": str(uuid4()),
            "transaction_id": str(uuid4()),
            "match": {
                "bundle_uuid": bundle_uuid,
                "bundle_version": bundle_version
            }
        }

    def reindex(self, sync: bool = False):
        errors = defaultdict(int)
        missing = {}
        indexed = 0
        total = 0

        logger.info('Querying DSS using %s', json.dumps(self.es_query, indent=4))
        response = self._post_dss_search()
        bundle_fqids = [r['bundle_fqid'] for r in response]
        logger.info("Bundle FQIDs to index: %i", len(bundle_fqids))

        with ThreadPoolExecutor(max_workers=self.num_workers, thread_name_prefix='pool') as tpe:

            def attempt(bundle_fqid, i):
                try:
                    logger.info("Bundle %s, attempt %i: Sending notification", bundle_fqid, i)
                    url = urlparse(self.indexer_url)
                    if sync:
                        # noinspection PyProtectedMember
                        url = url._replace(query=urlencode({**parse_qs(url.query),
                                                            'sync': sync}, doseq=True))
                    if not self.dryrun:
                        self.post_bundle(bundle_fqid=bundle_fqid, indexer_url=url.geturl())
                except HTTPError as e:
                    if i < 3:
                        logger.warning("Bundle %s, attempt %i: scheduling retry after error %s", bundle_fqid, i, e)
                        return bundle_fqid, tpe.submit(partial(attempt, bundle_fqid, i + 1))
                    else:
                        logger.warning("Bundle %s, attempt %i: giving up after error %s", bundle_fqid, i, e)
                        return bundle_fqid, e
                else:
                    logger.info("Bundle %s, attempt %i: success", bundle_fqid, i)
                    return bundle_fqid, None

            def handle_future(future):
                nonlocal indexed
                # Block until future raises or succeeds
                exception = future.exception()
                if exception is None:
                    bundle_fqid, result = future.result()
                    if result is None:
                        indexed += 1
                    elif isinstance(result, HTTPError):
                        errors[result.code] += 1
                        missing[bundle_fqid] = result.code
                    elif isinstance(result, Future):
                        # The task scheduled a follow-on task, presumably a retry. Follow that new task.
                        handle_future(result)
                    else:
                        assert False
                else:
                    logger.warning("Unhandled exception in worker:", exc_info=exception)

            futures = []
            for bundle_fqid in bundle_fqids:
                total += 1
                futures.append(tpe.submit(partial(attempt, bundle_fqid, 0)))
            for future in futures:
                handle_future(future)

        printer = PrettyPrinter(stream=None, indent=1, width=80, depth=None, compact=False)
        logger.info("Total of bundle FQIDs read: %i", total)
        logger.info("Total of bundle FQIDs indexed: %i", indexed)
        logger.warning("Total number of errors by code:\n%s", printer.pformat(dict(errors)))
        logger.warning("Missing bundle_fqids and their error code:\n%s", printer.pformat(missing))

    def _post_dss_search(self):
        return self.dss_client.post_search.iterate(es_query=self.es_query, replica='aws', per_page=500)

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

    def remote_reindex(self, bundle_uuid_prefix_length):
        prefixes = map(''.join, product('0123456789abcdef', repeat=bundle_uuid_prefix_length))

        def message(prefix):
            logger.info("Preparing message for prefix %s", prefix)
            query = deepcopy(self.es_query)
            must = query['query']['bool']['must']
            assert isinstance(must, list)
            must.append({'prefix': {'uuid': prefix}})
            return dict(action='reindex', dss_url=self.dss_url, query=query, dryrun=self.dryrun, prefix=prefix)

        notify_queue = self.queue(config.notify_queue_name)
        messages = map(message, prefixes)
        for batch in chunked(messages, 10):
            notify_queue.send_messages(Entries=[dict(Id=str(i), MessageBody=json.dumps(message))
                                                for i, message in enumerate(batch)])

    @classmethod
    def do_remote_reindex(cls, message):
        self = cls(dss_url=message['dss_url'],
                   es_query=message['query'],
                   dryrun=message['dryrun'])
        prefix=message['prefix']
        response = self._post_dss_search()
        # Render list of bundle FQIDs before moving on, reducing probability of duplicate notifications on retries
        bundle_fqids = [r['bundle_fqid'].partition('.') for r in response]
        logger.info("DSS returned %i bundles for prefix %s", len(bundle_fqids), prefix)
        messages = (dict(action='add', notification=self._make_notification(bundle_uuid, bundle_version))
                    for bundle_uuid, _, bundle_version in bundle_fqids)
        notify_queue = self.queue(config.notify_queue_name)
        num_messages = 0
        for batch in chunked(messages, 10):
            if not self.dryrun:
                notify_queue.send_messages(Entries=[dict(Id=str(i), MessageBody=json.dumps(message))
                                                    for i, message in enumerate(batch)])
            num_messages += len(batch)
        logger.info('Successfully queued %i notification(s) for prefix %s', num_messages, prefix)

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
