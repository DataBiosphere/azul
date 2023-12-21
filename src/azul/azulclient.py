from collections import (
    defaultdict,
)
from collections.abc import (
    Iterable,
    Set,
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
)
import json
import logging
from pprint import (
    PrettyPrinter,
)
from typing import (
    Union,
    cast,
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
)
from azul.es import (
    ESClientFactory,
)
from azul.hmac import (
    SignatureHelper,
)
from azul.indexer import (
    SourceJSON,
    SourceRef,
    SourcedBundleFQID,
)
from azul.indexer.index_service import (
    IndexService,
)
from azul.plugins import (
    RepositoryPlugin,
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

log = logging.getLogger(__name__)


@attr.s(frozen=True, auto_attribs=True, kw_only=True)
class AzulClient(SignatureHelper):
    num_workers: int = 16

    @cache
    def repository_plugin(self, catalog: CatalogName) -> RepositoryPlugin:
        return RepositoryPlugin.load(catalog).create(catalog)

    def post_bundle(self, indexer_url: furl, notification):
        """
        Send a mock DSS notification to the indexer
        """
        request = requests.Request('POST', str(indexer_url), json=notification)
        response = self.sign_and_send(request)
        response.raise_for_status()
        return response.content

    def synthesize_notification(self, bundle_fqid: SourcedBundleFQID) -> JSON:
        """
        Generate an indexer notification for the given bundle.
        """
        # Organic notifications sent by DSS have a different structure,
        # but since DSS is end-of-life these synthetic notifications are now the
        # only variant that would ever occur in the wild.
        return {
            'transaction_id': str(uuid.uuid4()),
            'bundle_fqid': bundle_fqid.to_json()
        }

    def bundle_message(self,
                       catalog: CatalogName,
                       bundle_fqid: SourcedBundleFQID
                       ) -> JSON:
        return {
            'action': 'add',
            'notification': self.synthesize_notification(bundle_fqid),
            'catalog': catalog
        }

    def reindex_message(self,
                        catalog: CatalogName,
                        source: SourceRef,
                        prefix: str
                        ) -> JSON:
        return {
            'action': 'reindex',
            'catalog': catalog,
            'source': source.to_json(),
            'prefix': prefix
        }

    def reindex(self, catalog: CatalogName, prefix: str) -> int:
        notifications = [
            self.synthesize_notification(bundle_fqid)
            for source in self.catalog_sources(catalog)
            for bundle_fqid in self.list_bundles(catalog, source, prefix)
        ]
        self.index(catalog, notifications)
        return len(notifications)

    def index(self,
              catalog: CatalogName,
              notifications: Iterable[JSON],
              delete: bool = False
              ):
        errors = defaultdict(int)
        missing = []
        indexed = 0
        total = 0
        path = (catalog, 'delete' if delete else 'add')
        indexer_url = config.indexer_endpoint.set(path=path)

        with ThreadPoolExecutor(max_workers=self.num_workers, thread_name_prefix='pool') as tpe:

            def attempt(notification, i):
                log_args = (indexer_url, notification, i)
                try:
                    log.info('Notifying %s about %s, attempt %i.', *log_args)
                    self.post_bundle(indexer_url, notification)
                except (requests.HTTPError, requests.ConnectionError) as e:
                    if i < 3:
                        log.warning('Retrying to notify %s about %s, attempt %i, after error %s.', *log_args, e)
                        return notification, tpe.submit(partial(attempt, notification, i + 1))
                    else:
                        log.warning('Failed to notify %s about %s, attempt %i: after error %s.', *log_args, e)
                        return notification, e
                else:
                    log.info('Success notifying %s about %s, attempt %i.', *log_args)
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
        log.info('Sent notifications for %i of %i bundles for catalog %r.',
                 indexed, total, catalog)
        if errors:
            log.error('Number of errors by HTTP status code:\n%s',
                      printer.pformat(dict(errors)))
        if missing:
            log.error('Unsent notifications and their HTTP status code:\n%s',
                      printer.pformat(missing))
        if errors or missing:
            raise AzulClientNotificationError

    def catalog_sources(self, catalog: CatalogName) -> Set[str]:
        return set(map(str, self.repository_plugin(catalog).sources))

    def list_bundles(self,
                     catalog: CatalogName,
                     source: Union[str, SourceRef],
                     prefix: str
                     ) -> list[SourcedBundleFQID]:
        validate_uuid_prefix(prefix)
        plugin = self.repository_plugin(catalog)
        if isinstance(source, str):
            source = plugin.resolve_source(source)
        else:
            assert isinstance(source, SourceRef), source
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
                       sources: Set[str]):

        plugin = self.repository_plugin(catalog)
        for source in sources:
            source = plugin.resolve_source(source)

            def message(partition_prefix: str) -> JSON:
                log.info('Remotely reindexing prefix %r of source %r into catalog %r',
                         partition_prefix, str(source.spec), catalog)
                return self.reindex_message(catalog, source, partition_prefix)

            messages = map(message, source.spec.prefix.partition_prefixes())
            for batch in chunked(messages, 10):
                entries = [
                    dict(Id=str(i), MessageBody=json.dumps(message))
                    for i, message in enumerate(batch)
                ]
                self.notifications_queue.send_messages(Entries=entries)

    def remote_reindex_partition(self, message: JSON) -> None:
        catalog = message['catalog']
        prefix = message['prefix']
        # FIXME: Adopt `trycast` for casting JSON to TypeDict
        #        https://github.com/DataBiosphere/azul/issues/5171
        source = cast(SourceJSON, message['source'])
        validate_uuid_prefix(prefix)
        source = self.repository_plugin(catalog).source_from_json(source)
        bundle_fqids = self.list_bundles(catalog, source, prefix)
        bundle_fqids = self.filter_obsolete_bundle_versions(bundle_fqids)
        log.info('After filtering obsolete versions, '
                 '%i bundles remain in prefix %r of source %r in catalog %r',
                 len(bundle_fqids), prefix, str(source.spec), catalog)
        messages = (
            self.bundle_message(catalog, bundle_fqid)
            for bundle_fqid in bundle_fqids
        )
        num_messages = self.queue_notifications(messages)
        log.info('Successfully queued %i notification(s) for prefix %s of '
                 'source %r', num_messages, prefix, source)

    def queue_notifications(self, messages: Iterable[JSON]) -> int:
        num_messages = 0
        for batch in chunked(messages, 10):
            entries = [
                dict(Id=str(i), MessageBody=json.dumps(message))
                for i, message in enumerate(batch)
            ]
            self.notifications_queue.send_messages(Entries=entries)
            num_messages += len(batch)
        return num_messages

    @classmethod
    def filter_obsolete_bundle_versions(cls,
                                        bundle_fqids: Iterable[SourcedBundleFQID]
                                        ) -> list[SourcedBundleFQID]:
        """
        Suppress obsolete bundle versions by only taking the latest version for
        each bundle UUID.
        >>> AzulClient.filter_obsolete_bundle_versions([])
        []
        >>> from azul.indexer import SimpleSourceSpec, SourceRef, Prefix
        >>> p = Prefix.parse('/2')
        >>> s = SourceRef(id='i', spec=SimpleSourceSpec(prefix=p, name='n'))
        >>> def b(u, v):
        ...     return SourcedBundleFQID(source=s, uuid=u, version=v)
        >>> AzulClient.filter_obsolete_bundle_versions([
        ...     b('c', '0'),
        ...     b('a', '1'),
        ...     b('b', '3')
        ... ]) # doctest: +NORMALIZE_WHITESPACE
        [SourcedBundleFQID(uuid='c',
                           version='0',
                           source=SourceRef(id='i',
                                            spec=SimpleSourceSpec(prefix=Prefix(common='',
                                                                                partition=2),
                                                                  name='n'))),
        SourcedBundleFQID(uuid='b',
                          version='3',
                          source=SourceRef(id='i',
                                           spec=SimpleSourceSpec(prefix=Prefix(common='',
                                                                               partition=2),
                                                                 name='n'))),
        SourcedBundleFQID(uuid='a',
                          version='1',
                          source=SourceRef(id='i',
                                           spec=SimpleSourceSpec(prefix=Prefix(common='',
                                                                               partition=2),
                                                                 name='n')))]
        >>> AzulClient.filter_obsolete_bundle_versions([
        ...     b('C', '0'), b('a', '1'), b('a', '0'),
        ...     b('a', '2'), b('b', '1'), b('c', '2')
        ... ]) # doctest: +NORMALIZE_WHITESPACE
        [SourcedBundleFQID(uuid='c',
                           version='2',
                           source=SourceRef(id='i',
                                            spec=SimpleSourceSpec(prefix=Prefix(common='',
                                                                                partition=2),
                                                                  name='n'))),
        SourcedBundleFQID(uuid='b',
                          version='1',
                          source=SourceRef(id='i',
                                           spec=SimpleSourceSpec(prefix=Prefix(common='',
                                                                               partition=2),
                                                                 name='n'))),
        SourcedBundleFQID(uuid='a',
                          version='2',
                          source=SourceRef(id='i',
                                           spec=SimpleSourceSpec(prefix=Prefix(common='',
                                                                               partition=2),
                                                                 name='n')))]
        >>> AzulClient.filter_obsolete_bundle_versions([
        ...     b('a', '0'), b('A', '1')
        ... ]) # doctest: +NORMALIZE_WHITESPACE
        [SourcedBundleFQID(uuid='A',
                           version='1',
                           source=SourceRef(id='i',
                                            spec=SimpleSourceSpec(prefix=Prefix(common='',
                                                                                partition=2),
                                                                  name='n')))]
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
                fqid.source.id.lower(),
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
        log.info('Deleting bundle %r, version %r in catalog %r.',
                 bundle_uuid, bundle_version, catalog)
        notifications = [
            {
                # FIXME: delete_bundle script fails with KeyError: 'source'
                #        https://github.com/DataBiosphere/azul/issues/5105
                'bundle_fqid': {
                    'uuid': bundle_uuid,
                    'version': bundle_version
                }
            }
        ]
        self.index(catalog, notifications, delete=True)

    def deindex(self, catalog: CatalogName, sources: Iterable[str]):
        plugin = self.repository_plugin(catalog)
        source_ids = [plugin.resolve_source(s).id for s in sources]
        es_client = ESClientFactory.get()
        indices = ','.join(map(str, self.index_service.index_names(catalog)))
        query = {
            'query': {
                'bool': {
                    'should': [
                        {
                            'terms': {
                                # Aggregate documents
                                'sources.id.keyword': source_ids
                            }
                        },
                        {
                            'terms': {
                                # Contribution documents
                                'source.id.keyword': source_ids
                            }
                        }
                    ]
                }
            }
        }
        log.info('Deindexing sources %r from catalog %r', sources, catalog)
        log.debug('Using query: %r', query)
        response = es_client.delete_by_query(index=indices, body=query, slices='auto')
        if len(response['failures']) > 0:
            if response['version_conflicts'] > 0:
                log.error('Version conflicts encountered. Do not deindex while '
                          'indexing is occurring. The index may now be in an '
                          'inconsistent state.')
            raise RuntimeError('Failures during deletion', response['failures'])

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
            log.info('Disabling lambdas ...')
            self.queues.manage_lambdas(work_queues, enable=False)
            log.info('Purging queues: %s', ', '.join(work_queues.keys()))
            self.queues.purge_queues_unsafely(work_queues)
        if delete_indices:
            log.info('Deleting indices ...')
            for catalog in catalogs:
                self.delete_all_indices(catalog)
        if purge_queues:
            log.info('Re-enabling lambdas ...')
            self.queues.manage_lambdas(work_queues, enable=True)
        if create_indices:
            log.info('Creating indices ...')
            for catalog in catalogs:
                self.create_all_indices(catalog)

    def wait_for_indexer(self):
        """
        Wait for indexer to begin processing notifications, then wait for work
        to finish.
        """
        self.queues.wait_to_stabilize()


class AzulClientError(RuntimeError):
    pass


class AzulClientNotificationError(AzulClientError):

    def __init__(self) -> None:
        super().__init__('Some notifications could not be sent')
