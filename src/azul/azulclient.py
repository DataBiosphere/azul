from collections import (
    defaultdict,
)
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
    starmap,
)
import json
import logging
from pprint import (
    PrettyPrinter,
)
from typing import (
    AbstractSet,
    Iterable,
    List,
)
import uuid

import attr
from furl import (
    furl,
)
from more_itertools import (
    chunked,
)
import requests

from azul import (
    CatalogName,
    cache,
    cached_property,
    config,
    hmac,
)
from azul.indexer import (
    SourcedBundleFQID,
)
from azul.indexer.index_service import (
    IndexService,
)
from azul.plugins import (
    RepositoryPlugin,
)
from azul.plugins.repository import (
    dss,
)
from azul.queues import (
    Queues,
)
from azul.types import (
    JSON,
)
from azul.uuids import (
    validate_uuid_prefix,
)

logger = logging.getLogger(__name__)


@attr.s(frozen=True, auto_attribs=True, kw_only=True)
class AzulClient(object):
    num_workers: int = 16

    @cache
    def repository_plugin(self, catalog: CatalogName) -> RepositoryPlugin:
        return RepositoryPlugin.load(catalog).create(catalog)

    def query(self, catalog: CatalogName, prefix: str) -> JSON:
        validate_uuid_prefix(prefix)
        return self.repository_plugin(catalog).dss_subscription_query(prefix)

    def post_bundle(self, indexer_url, notification):
        """
        Send a mock DSS notification to the indexer
        """
        response = requests.post(indexer_url, json=notification, auth=hmac.prepare())
        response.raise_for_status()
        return response.content

    def synthesize_notification(self,
                                catalog: CatalogName,
                                prefix: str,
                                bundle_fqid: SourcedBundleFQID) -> JSON:
        """
        Generate a indexer notification for the given bundle.

        The returned notification is considered synthetic in contrast to the
        organic ones sent by DSS. They can be easily identified by the special
        subscription UUID.
        """
        # Organic notifications sent by DSS wouldn't contain the `source` entry,
        # but since DSS is end-of-life these synthetic notifications are now the
        # only variant that would ever occur in the wild.
        assert bundle_fqid.uuid.startswith(prefix)
        return {
            'source': {
                'id': bundle_fqid.source.id,
                'name': str(bundle_fqid.source.name),
            },
            'query': self.query(catalog, prefix),
            'subscription_id': 'cafebabe-feed-4bad-dead-beaf8badf00d',
            'transaction_id': str(uuid.uuid4()),
            'match': {
                'bundle_uuid': bundle_fqid.uuid,
                'bundle_version': bundle_fqid.version
            },
        }

    def reindex(self, catalog: CatalogName, prefix: str) -> int:
        notifications = [
            self.synthesize_notification(catalog=catalog,
                                         prefix=prefix,
                                         bundle_fqid=bundle_fqid)
            for source in self.catalog_sources(catalog)
            for bundle_fqid in self.list_bundles(catalog, source, prefix)
        ]
        self.index(catalog, notifications)
        return len(notifications)

    def bundle_has_project_json(self,
                                catalog: CatalogName,
                                bundle_fqid: SourcedBundleFQID
                                ) -> bool:
        plugin = self.repository_plugin(catalog)
        if isinstance(plugin, dss.Plugin):
            manifest = plugin.fetch_bundle_manifest(bundle_fqid)
            # Since we now use DSS' GET /bundles/all which doesn't support
            # filtering, we need to filter by hand.
            return any(f['name'] == 'project_0.json' and f['indexed'] for f in manifest)
        else:
            # Other plugins don't support the method and we'll just assume that
            # every bundle references a project.
            return True

    def index(self, catalog: CatalogName, notifications: Iterable[JSON], delete: bool = False):
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
        logger.info("Sent notifications for %i of %i bundles for catalog %r.",
                    indexed, total, catalog)
        if errors:
            logger.error("Number of errors by HTTP status code:\n%s",
                         printer.pformat(dict(errors)))
        if missing:
            logger.error("Unsent notifications and their HTTP status code:\n%s",
                         printer.pformat(missing))
        if errors or missing:
            raise AzulClientNotificationError

    def catalog_sources(self, catalog: CatalogName) -> AbstractSet[str]:
        return set(map(str, self.repository_plugin(catalog).sources))

    def list_bundles(self,
                     catalog: CatalogName,
                     source: str,
                     prefix: str
                     ) -> List[SourcedBundleFQID]:
        validate_uuid_prefix(prefix)
        plugin = self.repository_plugin(catalog)
        source = plugin.resolve_source(name=source)
        return plugin.list_bundles(source, prefix)

    @property
    def sqs(self):
        from azul.deployment import (
            aws,
        )
        return aws.resource('sqs')

    @cached_property
    def notifications_queue(self):
        return self.sqs.get_queue_by_name(QueueName=config.notifications_queue_name())

    def remote_reindex(self,
                       catalog: CatalogName,
                       prefix: str,
                       partition_prefix_length: int):
        validate_uuid_prefix(prefix)
        partition_prefixes = [
            prefix + ''.join(partition_prefix)
            for partition_prefix in product('0123456789abcdef',
                                            repeat=partition_prefix_length)
        ]
        sources = self.repository_plugin(catalog).sources

        def message(source: str, partition_prefix: str) -> JSON:
            logger.info('Remotely reindexing prefix %r of source %r into catalog %r',
                        partition_prefix, source, catalog)
            return dict(action='reindex',
                        catalog=catalog,
                        source=str(source),
                        prefix=partition_prefix)

        messages = starmap(message, product(sources, partition_prefixes))
        for batch in chunked(messages, 10):
            entries = [
                dict(Id=str(i), MessageBody=json.dumps(message))
                for i, message in enumerate(batch)
            ]
            self.notifications_queue.send_messages(Entries=entries)

    def remote_reindex_partition(self, message: JSON) -> None:
        catalog = message['catalog']
        prefix = message['prefix']
        validate_uuid_prefix(prefix)
        source = message['source']
        bundle_fqids = self.list_bundles(catalog, source, prefix)
        bundle_fqids = self.filter_obsolete_bundle_versions(bundle_fqids)
        logger.info('After filtering obsolete versions, '
                    '%i bundles remain in prefix %r of catalog %r',
                    len(bundle_fqids), prefix, catalog)
        messages = (
            {
                'action': 'add',
                'notification': self.synthesize_notification(catalog=catalog,
                                                             prefix=prefix,
                                                             bundle_fqid=bundle_fqid),
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
        logger.info('Successfully queued %i notification(s) for prefix %s', num_messages, prefix)

    @classmethod
    def filter_obsolete_bundle_versions(cls,
                                        bundle_fqids: Iterable[SourcedBundleFQID]
                                        ) -> List[SourcedBundleFQID]:
        """
        Suppress obsolete bundle versions by only taking the latest version for
        each bundle UUID.

        >>> AzulClient.filter_obsolete_bundle_versions([])
        []

        >>> from azul.indexer import SimpleSourceName, SourceRef
        >>> s = SourceRef(id='i', name=SimpleSourceName('n'))
        >>> def b(u, v):
        ...     return SourcedBundleFQID(source=s, uuid=u, version=v)
        >>> AzulClient.filter_obsolete_bundle_versions([
        ...     b('c', '0'),
        ...     b('a', '1'),
        ...     b('b', '3')
        ... ]) # doctest: +NORMALIZE_WHITESPACE
        [SourcedBundleFQID(uuid='c', version='0', source=SourceRef(id='i', name='n')), \
        SourcedBundleFQID(uuid='b', version='3', source=SourceRef(id='i', name='n')), \
        SourcedBundleFQID(uuid='a', version='1', source=SourceRef(id='i', name='n'))]

        >>> AzulClient.filter_obsolete_bundle_versions([
        ...     b('C', '0'), b('a', '1'), b('a', '0'),
        ...     b('a', '2'), b('b', '1'), b('c', '2')
        ... ]) # doctest: +NORMALIZE_WHITESPACE
        [SourcedBundleFQID(uuid='c', version='2', source=SourceRef(id='i', name='n')), \
        SourcedBundleFQID(uuid='b', version='1', source=SourceRef(id='i', name='n')), \
        SourcedBundleFQID(uuid='a', version='2', source=SourceRef(id='i', name='n'))]

        >>> AzulClient.filter_obsolete_bundle_versions([
        ...     b('a', '0'), b('A', '1')
        ... ])
        [SourcedBundleFQID(uuid='A', version='1', source=SourceRef(id='i', name='n'))]
        """

        # Sort lexicographically by source and FQID. I've observed the DSS
        # response to already be in this order
        def sort_key(fqid: SourcedBundleFQID):
            return (
                fqid.source,
                fqid.uuid.lower(),
                fqid.version.lower()
            )

        bundle_fqids = sorted(bundle_fqids, key=sort_key, reverse=True)

        # Group by source and bundle UUID
        def group_key(fqid: SourcedBundleFQID):
            return (
                fqid.source,
                fqid.uuid.lower()
            )

        bundle_fqids = groupby(bundle_fqids, key=group_key)

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
        logger.info('Deleting bundle %r, version %r in catalog %r.',
                    bundle_uuid, bundle_version, catalog)
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
                      catalogs: Iterable[CatalogName],
                      *,
                      purge_queues: bool,
                      delete_indices: bool,
                      create_indices: bool):
        """
        Reset the indexer, to a degree.

        :param catalogs: The catalogs to create and delete indices for.

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
            for catalog in catalogs:
                self.delete_all_indices(catalog)
        if purge_queues:
            logger.info('Re-enabling lambdas ...')
            self.queues.manage_lambdas(work_queues, enable=True)
        if create_indices:
            logger.info('Creating indices ...')
            for catalog in catalogs:
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
