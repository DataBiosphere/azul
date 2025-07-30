from itertools import (
    groupby,
)
import logging
from typing import (
    Iterable,
)

from azul import (
    CatalogName,
    JSON,
    cache,
)
from azul.indexer import (
    Bundle,
    SourceRef,
    SourcedBundleFQID,
)
from azul.plugins import (
    RepositoryPlugin,
)

log = logging.getLogger(__name__)


class IndexRepositoryService:

    @cache
    def repository_plugin(self, catalog: CatalogName) -> RepositoryPlugin:
        return RepositoryPlugin.load(catalog).create(catalog)

    def list_bundles(self,
                     catalog: CatalogName,
                     source: SourceRef,
                     prefix: str
                     ) -> list[SourcedBundleFQID]:
        plugin = self.repository_plugin(catalog)
        log.info('Listing bundles with prefix %r in source %r.', prefix, source)
        bundle_fqids = plugin.list_bundles(source, prefix)
        log.info('There are %i bundle(s) with prefix %r in source %r.',
                 len(bundle_fqids), prefix, source)
        return bundle_fqids

    def filter_obsolete_bundle_versions(self,
                                        bundle_fqids: Iterable[SourcedBundleFQID]
                                        ) -> list[SourcedBundleFQID]:
        """
        Suppress obsolete bundle versions by only taking the latest version for
        each bundle UUID.
        >>> service = IndexRepositoryService()
        >>> service.filter_obsolete_bundle_versions([])
        []
        >>> from azul.indexer import SimpleSourceSpec, SourceRef, Prefix
        >>> p = Prefix.parse('/2')
        >>> s = SourceRef(id='i', spec=SimpleSourceSpec(prefix=p, name='n'))
        >>> def b(u, v):
        ...     return SourcedBundleFQID(source=s, uuid=u, version=v)
        >>> service.filter_obsolete_bundle_versions([
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
        >>> service.filter_obsolete_bundle_versions([
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
        >>> service.filter_obsolete_bundle_versions([
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

        groups = groupby(bundle_fqids, key=group_key)

        # Take the first item in each group. Because the oder is reversed, this
        # is the latest version
        bundle_fqids = [next(group) for _, group in groups]
        return bundle_fqids

    def fetch_bundle(self, catalog: CatalogName, bundle_fqid: JSON) -> Bundle:
        plugin = self.repository_plugin(catalog)
        bundle_fqid = plugin.bundle_fqid_cls.from_json(bundle_fqid)
        return plugin.fetch_bundle(bundle_fqid)
