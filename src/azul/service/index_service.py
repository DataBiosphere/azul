from abc import (
    ABCMeta,
    abstractmethod,
)
from collections.abc import (
    Mapping,
    Sequence,
)
import json
import logging

import attrs
from more_itertools import (
    first,
    one,
)
from opensearchpy import (
    MultiSearch,
    Search,
)
from opensearchpy.helpers.response import (
    Hit,
)

from azul import (
    CatalogName,
    config,
)
from azul.filters import (
    Filters,
)
from azul.indexer.mirror_service import (
    MirrorService,
)
from azul.lib.types import (
    JSON,
    MutableJSON,
)
from azul.plugins import (
    File,
    dotted,
)
from azul.service import (
    BadArgumentException,
    FileUrlFunc,
)
from azul.service.query_service import (
    OpenSearchStage,
    Pagination,
    PaginationStage,
    QueryService,
    ResponseTriple,
    ToDictStage,
    _OpenSearchStage,
)
from azul.source import (
    SourceRef,
)

log = logging.getLogger(__name__)


@attrs.frozen(auto_attribs=True, kw_only=True)
class SearchResponseStage(_OpenSearchStage[ResponseTriple, MutableJSON],
                          metaclass=ABCMeta):
    service: IndexService
    file_url_func: FileUrlFunc

    def prepare_request(self, request: Search) -> Search:
        return request

    def _file_url(self, *, uuid: str, version: str, drs_uri: str | None) -> str | None:
        # FIXME: Redundant implementations of file URLs and mirror URIs
        #        https://github.com/DataBiosphere/azul/issues/8042
        if drs_uri is None:
            # To download a file we need its DRS URI
            return None
        elif drs_uri.startswith('drs://dg.4503'):
            # LungMAP contains files not hosted on TDR. Downloading these files
            # requires authentication that can't be provided by Azul.
            #
            # FIXME: We shouldn't hard-code compact identifier namespaces
            #        https://github.com/DataBiosphere/azul/issues/8236
            return None
        else:
            return str(self.file_url_func(catalog=self.catalog,
                                          fetch=False,
                                          file_uuid=uuid,
                                          version=version))

    def _file_mirror_uri(self, source: SourceRef, file: JSON) -> str | None:
        # FIXME: Redundant implementations of file URLs and mirror URIs
        #        https://github.com/DataBiosphere/azul/issues/8042
        file_cls = self.plugin.file_class
        mirror_service = MirrorService.for_catalog(self.catalog)
        return mirror_service.mirror_uri(source, file_cls, file)


class SummaryResponseStage(OpenSearchStage[JSON, MutableJSON],
                           metaclass=ABCMeta):

    @property
    @abstractmethod
    def response_schema(self) -> JSON:
        """
        The JSON schema of the response returned by :meth:`process_response`, to
        be used in the OpenAPI spec of the summary endpoint.
        """
        raise NotImplementedError

    @property
    @abstractmethod
    def response_description(self) -> str:
        """
        A description of the response returned by :meth:`process_response`, to be
        used in the OpenAPI spec of the summary endpoint.
        """
        raise NotImplementedError

    @property
    @abstractmethod
    def aggs_by_authority(self) -> Mapping[str, Sequence[str]]:
        """
        The name of each aggregation this stage consumes, by the entity type of
        the index that is authoritative for it. The response passed to
        :meth:`process_response` is keyed the same way, so the same aggregation
        may be taken from more than one index.
        """
        raise NotImplementedError

    def prepare_request(self, request: Search) -> Search:
        return request


class IndexService(QueryService):

    def search(self,
               *,
               catalog: CatalogName,
               entity_type: str,
               file_url_func: FileUrlFunc,
               aggregate: bool,
               filters: Filters,
               pagination: Pagination
               ) -> MutableJSON:
        """
        Search the index of the given entity type for entities matching the
        given filters, and return one page of the hits.

        :param catalog: the name of the catalog to search

        :param entity_type: which index to search, e.g. 'projects' or 'files'

        :param file_url_func: used to construct the URL of a data file in a hit

        :param aggregate: whether to tabulate the facets of all matching
                          entities, not just those on the returned page

        :param filters: the filters that matching entities must satisfy

        :param pagination: which page of the hits to return, and how to sort
                           them

        :return: the hits on that page, the pagination of the result set and,
                 if `aggregate` is set, the facets
        """
        plugin = self.metadata_plugin(catalog)
        field_mapping = plugin.field_mapping

        for field in filters.explicit.keys():
            accessible_field = plugin.special_fields.accessible.name
            if field != accessible_field and field not in field_mapping:
                raise BadArgumentException(f'Unable to filter by undefined field {field}.')

        field = pagination.sort
        if field not in field_mapping:
            raise BadArgumentException(f'Unable to sort by undefined field {field}.')

        chain = self.create_chain(catalog=catalog,
                                  entity_type=entity_type,
                                  filters=filters,
                                  post_filter=True,
                                  document_slice=None)

        chain = ToDictStage(service=self,
                            catalog=catalog,
                            entity_type=entity_type).wrap(chain)

        if aggregate:
            chain = plugin.aggregation_stage.create_and_wrap(chain)

        chain = PaginationStage(service=self,
                                catalog=catalog,
                                entity_type=entity_type,
                                pagination=pagination,
                                peek_ahead=True,
                                filters=filters).wrap(chain)

        response_stage_cls = plugin.search_response_stage
        chain = response_stage_cls(service=self,
                                   catalog=catalog,
                                   entity_type=entity_type,
                                   file_url_func=file_url_func).wrap(chain)

        request = self.create_request(catalog, entity_type)
        request = chain.prepare_request(request)
        response = request.execute(ignore_cache=True)
        response = chain.process_response(response)

        special_fields = self.metadata_plugin(catalog).special_fields
        for hit in response['hits']:
            entity = one(hit[entity_type])
            source_id = one(hit['sources'])[special_fields.source_id.name_in_hit]
            accessible = source_id in filters.source_ids
            entity[special_fields.accessible.name_in_hit] = accessible

        return response

    def summary(self,
                catalog: CatalogName,
                filters: Filters
                ) -> MutableJSON:
        # FIXME: Due to the fact that we run one chain per authority, and the
        #        resulting need to multiplex the responses, the response stage
        #        is not part of any chain.
        #        https://github.com/DataBiosphere/azul/issues/4128
        plugin = self.metadata_plugin(catalog)
        response_stage = plugin.summary_response_stage()

        aggs_by_authority = response_stage.aggs_by_authority

        aggs = self._summary(catalog=catalog,
                             entity_types=list(aggs_by_authority),
                             filters=filters)

        aggs = {
            entity_type: {
                agg_name: aggs[entity_type][agg_name]
                for agg_name in summary_fields
            }
            for entity_type, summary_fields in aggs_by_authority.items()
        }

        response = response_stage.process_response(aggs)
        return response

    def _summary(self,
                 *,
                 catalog: CatalogName,
                 entity_types: Sequence[str],
                 filters: Filters
                 ) -> Mapping[str, MutableJSON]:
        """
        Aggregate the index of each of the given entity types, in one request,
        and return the aggregations of each index, by entity type. One chain
        per entity type prepares its own search and processes the response to
        it; the searches merely travel together.
        """
        plugin = self.metadata_plugin(catalog)
        chains = {}
        request = MultiSearch(using=self._opensearch)
        for entity_type in entity_types:
            chain = self.create_chain(catalog=catalog,
                                      entity_type=entity_type,
                                      filters=filters,
                                      post_filter=False,
                                      document_slice=None)
            chain = ToDictStage(service=self,
                                catalog=catalog,
                                entity_type=entity_type).wrap(chain)
            chain = plugin.summary_aggregation_stage.create_and_wrap(chain)
            chains[entity_type] = chain
            request = request.add(
                chain.prepare_request(self.create_request(catalog, entity_type)))

        if config.debug == 2 and log.isEnabledFor(logging.DEBUG):
            log.debug('OpenSearch request: %s', json.dumps(request.to_dict(), indent=4))

        responses = request.execute(ignore_cache=True)

        result = {}
        for (entity_type, chain), response in zip(chains.items(),
                                                  responses,
                                                  strict=True):
            assert len(response.hits) == 0, entity_type
            result[entity_type] = chain.process_response(response)

        return result

    def get_data_file(self,
                      catalog: CatalogName,
                      file_uuid: str,
                      file_version: str | None,
                      filters: Filters,
                      ) -> File | None:
        """
        Return the inner `files` entity describing the data file with the
        given UUID and version.

        :param catalog: the catalog to search in

        :param file_uuid: the UUID of the data file

        :param file_version: the version of the data file, if absent the most
                             recent version will be returned

        :param filters: parsed filters from the request

        :return: The inner `files` entity or None if the catalog does not
                 contain information about the specified data file
        """
        plugin = self.metadata_plugin(catalog)
        file_uuid_field = plugin.special_fields.file_uuid
        filters = filters.set_explicit({
            file_uuid_field.name: {'is': [file_uuid]},
            **(
                {'fileVersion': {'is': [file_version]}}
                if file_version is not None else
                {}
            )
        })

        def _hit_to_doc(hit: Hit) -> JSON:
            return self.translate_fields(catalog, hit.to_dict(), forward=False)

        entity_type = 'files'
        chain = self.create_chain(catalog=catalog,
                                  entity_type=entity_type,
                                  filters=filters,
                                  post_filter=False,
                                  document_slice=None)
        request = self.create_request(catalog, entity_type)
        request = chain.prepare_request(request)

        if file_version is None:
            field_path = dotted(plugin.field_mapping['fileVersion'])
            request.sort({field_path: dict(order='desc')})

        # Just need two hits to detect an ambiguous response
        request.params(size=2)

        hits = list(map(_hit_to_doc, request.execute().hits))

        if len(hits) == 0:
            return None
        elif len(hits) > 1:
            # Can't have more than one hit with the same version
            assert file_version is None, len(hits)

        hit = first(hits)
        source = SourceRef.from_json(one(hit['sources']))
        file = one(hit['contents']['files'])
        file = plugin.file_class.from_index(file, source=source)
        if file_version is not None:
            assert file_version == file.version
        return file

    @property
    def always_limit_access(self) -> bool:
        return False
