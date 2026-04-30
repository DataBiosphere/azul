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
import fnmatch
from functools import (
    partial,
)
import logging
from pprint import (
    PrettyPrinter,
)
import uuid

import attrs
import requests
from urllib3 import (
    BaseHTTPResponse,
    HTTPResponse,
)
from urllib3.exceptions import (
    HTTPError,
)

from azul import (
    CatalogName,
    config,
)
from azul.hmac import (
    SignatureHelper,
)
from azul.http import (
    HasCachedHttpClient,
)
from azul.indexer.index_queue_service import (
    IndexQueueService,
)
from azul.indexer.index_service import (
    IndexService,
)
from azul.indexer.mirror_service import (
    MirrorService,
)
from azul.indexer.repository_service import (
    RepositoryService,
)
from azul.lib import (
    R,
    cache,
    cached_property,
)
from azul.lib.types import (
    JSON,
    JSONs,
)
from azul.opensearch import (
    OpenSearchClientFactory,
)
from azul.plugins import (
    MetadataPlugin,
    RepositoryPlugin,
)
from azul.queues import (
    Queues,
)
from azul.service.source_service import (
    SourceService,
)
from azul.source import (
    SourceConfig,
    SourceSpec,
)

log = logging.getLogger(__name__)


@attrs.frozen(kw_only=True)
class AzulClient(SignatureHelper, HasCachedHttpClient):
    num_workers: int = 16

    @cached_property
    def queues(self) -> Queues:
        return Queues()

    @cached_property
    def index_service(self) -> IndexService:
        return IndexService()

    @cached_property
    def index_queue_service(self) -> IndexQueueService:
        return IndexQueueService()

    @cached_property
    def repository_service(self) -> RepositoryService:
        return RepositoryService()

    def repository_plugin(self, catalog: CatalogName) -> RepositoryPlugin:
        return self.repository_service.repository_plugin(catalog)

    def metadata_plugin(self, catalog: CatalogName) -> MetadataPlugin:
        return self.index_service.metadata_plugin(catalog)

    @cache
    def mirror_service(self, catalog: CatalogName) -> MirrorService:
        return MirrorService(catalog=catalog)

    @cached_property
    def source_service(self) -> SourceService:
        return SourceService()

    def local_reindex(self, catalog: CatalogName, prefix: str) -> int:
        service = self.repository_service
        plugin = self.repository_plugin(catalog)
        notifications: JSONs = [
            # Notifications sent organically by DSS had a different structure,
            # but since DSS is long gone these synthetic notifications are now
            # the only variant that would ever occur in the wild.
            {
                'transaction_id': str(uuid.uuid4()),
                'bundle_fqid': bundle_fqid.to_json()
            }
            for source in map(plugin.resolve_source, plugin.sources)
            for bundle_fqid in service.list_bundles(catalog, source, prefix)
        ]
        self.index(catalog, notifications)
        return len(notifications)

    def index(self,
              catalog: CatalogName,
              notifications: Iterable[JSON],
              delete: bool = False
              ):
        errors = defaultdict[int, int](int)
        missing = []
        indexed = 0
        total = 0
        path = (catalog, 'bundles')
        indexer_url = config.indexer_endpoint.set(path=path)

        def attempt(notification: JSON,
                    i: int
                    ) -> tuple[JSON, None | Future | HTTPResponse | HTTPError]:
            log_args = (indexer_url, notification, i)
            log.info('Notifying %s about %s, attempt %i.',
                     *log_args)
            # We want to send the request with urllib3 directly but HMAC
            # signing is only available for Requests, so we need to prepare a
            # request, sign it and then unpack it again before calling urllib3.
            request = requests.models.Request(method='DELETE' if delete else 'POST',
                                              url=str(indexer_url),
                                              json=notification)
            request = request.prepare()
            self.sign(request)
            result: BaseHTTPResponse | HTTPError
            try:
                result = self._http_client.request(url=request.url,
                                                   method=request.method,
                                                   headers=request.headers,
                                                   body=request.body)
            except HTTPError as e:
                result = e

            if isinstance(result, BaseHTTPResponse) and result.status == 202:
                log.info('Success notifying %s about %s, attempt %i.',
                         *log_args)
                return notification, None
            else:
                assert isinstance(result, (HTTPResponse, HTTPError)), result
                if i < 3:
                    log.warning('Retrying to notify %s about %s, attempt %i, after error %s.',
                                *log_args, result)
                    return notification, tpe.submit(partial(attempt, notification, i + 1))
                else:
                    log.warning('Failed to notify %s about %s, attempt %i: after error %s.',
                                *log_args, result)
                    return notification, result

        def handle_future(future: Future) -> None:
            nonlocal indexed
            bundle_fqid, result = future.result()
            if result is None:
                indexed += 1
            elif isinstance(result, HTTPResponse):
                errors[result.status] += 1
                missing.append((notification, result.status))
            elif isinstance(result, Future):
                # The task scheduled a follow-on task, presumably a retry.
                # Follow that new task.
                handle_future(result)
            else:
                assert False

        with ThreadPoolExecutor(max_workers=self.num_workers,
                                thread_name_prefix='pool') as tpe:
            futures = []
            for notification in notifications:
                total += 1
                futures.append(tpe.submit(partial(attempt, notification, 0)))
            for future in futures:
                handle_future(future)

        printer = PrettyPrinter(compact=False)
        log.info('Sent notifications for %i of %i bundles for catalog %r.',
                 indexed, total, catalog)
        if errors:
            log.error('Number of errors by HTTP status code:\n%s',
                      printer.pformat(dict(errors)))
        if missing:
            log.error('Unsent notifications and their HTTP status code:\n%s',
                      printer.pformat(missing))
        if errors or missing:
            raise AzulClientNotificationError(set(errors.keys()))

    def matching_sources(self,
                         catalogs: Iterable[CatalogName],
                         globs: Set[str] = frozenset('*')
                         ) -> dict[CatalogName, dict[SourceSpec, SourceConfig]]:
        result = {}
        matched_globs: set[str] = set()
        for catalog in catalogs:
            raw_specs = config.sources(catalog)
            specs = dict(self.repository_plugin(catalog).sources)
            if '*' in globs:
                matched_globs.update(globs)
            else:
                matching_raw_specs: set[str] = set()
                for glob in globs:
                    _matching_raw_specs = fnmatch.filter(raw_specs, glob)
                    if _matching_raw_specs:
                        matching_raw_specs.update(_matching_raw_specs)
                        matched_globs.add(glob)
                        log.debug('Source glob %r matched sources %r in catalog %r',
                                  glob, _matching_raw_specs, catalog)
                specs = {spec: cfg for spec, cfg in specs.items() if str(spec) in matching_raw_specs}
            result[catalog] = specs
        unmatched_globs = globs - matched_globs
        if unmatched_globs:
            log.warning('Source(s) not found in any catalog: %r', unmatched_globs)
        assert any(result.values()), R(
            'No valid sources specified for any catalog')
        return result

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

    def deindex(self, catalog: CatalogName, sources: Iterable[SourceSpec]):
        plugin = self.repository_plugin(catalog)
        source_ids = [plugin.resolve_source(s).id for s in sources]
        opensearch = OpenSearchClientFactory.get()
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
        response = opensearch.delete_by_query(index=indices, body=query, slices='auto')
        if len(response['failures']) > 0:
            if response['version_conflicts'] > 0:
                log.error('Version conflicts encountered. Do not deindex while '
                          'indexing is occurring. The index may now be in an '
                          'inconsistent state.')
            raise RuntimeError('Failures during deletion', response['failures'])

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
        indexer_queues = self.queues.get_queues(config.indexer_work_queue_names)
        if purge_queues:
            log.info('Disabling lambdas ...')
            self.queues.manage_lambdas(indexer_queues, enable=False)
            log.info('Purging queues: %s', ', '.join(indexer_queues.keys()))
            self.queues.purge_queues_unsafely(indexer_queues)
        if delete_indices:
            log.info('Deleting indices ...')
            for catalog in catalogs:
                self.delete_all_indices(catalog)
        if purge_queues:
            log.info('Re-enabling lambdas ...')
            self.queues.manage_lambdas(indexer_queues, enable=True)
        if create_indices:
            log.info('Creating indices ...')
            for catalog in catalogs:
                self.create_all_indices(catalog)

    def wait_for_indexer(self):
        """
        Wait for indexer to begin processing notifications, then wait for work
        to finish.
        """
        # Indexing can still succeed after a transient stall. A stall's
        # transience cannot be proven until all lambdas and their respective
        # retries repeatedly time out, but this would result in an unreasonably
        # long wait time. Waiting for just one retry is sufficient to
        # accommodate the most probable scenarios for transient stalls.
        timeout = max(config.contribution_lambda_timeout(retry=True),
                      config.aggregation_lambda_timeout(retry=True))
        self.queues.wait_to_stabilize(config.indexer_work_queue_names,
                                      timeout,
                                      detect_stall=True)

    def wait_for_mirroring(self):
        self.queues.wait_to_stabilize(config.mirror_work_queue_names,
                                      config.mirror_lambda_timeout,
                                      detect_stall=False)

    def is_queue_empty(self, queue_name: str) -> bool:
        queues = self.queues.get_queues([queue_name])
        length, _ = self.queues.get_queue_lengths(queues)
        return length == 0

    def _get_non_empty_fail_queues(self) -> set[str]:
        return {
            queue
            for queue in config.indexer_fail_queue_names
            if not self.is_queue_empty(queue)
        }

    _common_fail_queue_msg = (
        "If needed, empty the work queues via 'manage_queues.py purge_indexer'. "
        "Then run 'manage_queues.py dump --delete' for each fail queue listed: "
    )

    def require_no_failures_before(self):
        queues = self._get_non_empty_fail_queues()
        assert 0 == len(queues), R(
            'Cannot begin indexing because a previous operation failed: '
            'At least one fail queue is not empty. ' +
            self._common_fail_queue_msg,
            queues
        )

    def require_no_failures_after(self):
        queues = self._get_non_empty_fail_queues()
        assert 0 == len(queues), R(
            'At least one fail queue is not empty, indicating that there were '
            'persistent indexer failures. ' +
            self._common_fail_queue_msg,
            queues
        )


class AzulClientError(RuntimeError):
    pass


class AzulClientNotificationError(AzulClientError):

    def __init__(self, *args) -> None:
        super().__init__('Some notifications could not be sent', *args)
