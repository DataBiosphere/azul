from abc import (
    ABCMeta,
    abstractmethod,
)
from collections.abc import (
    Mapping,
    Sequence,
)
from concurrent.futures import (
    ThreadPoolExecutor,
)
import json
import logging

import attrs
from more_itertools import (
    first,
    one,
)
import opensearchpy
from opensearchpy import (
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
from azul.lib.uuids import (
    validate_uuid,
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
    IndexNotFoundError,
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


class EntityNotFoundError(Exception):

    def __init__(self, entity_type: str, entity_id: str):
        super().__init__(f"Can't find an entity in {entity_type} with an uuid, {entity_id}.")


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
    def aggs_by_authority(self) -> Mapping[str, Sequence[str]]:
        raise NotImplementedError

    def prepare_request(self, request: Search) -> Search:
        return request


class IndexService(QueryService):

    def search(self,
               *,
               catalog: CatalogName,
               entity_type: str,
               file_url_func: FileUrlFunc,
               item_id: str | None,
               filters: Filters,
               pagination: Pagination
               ) -> MutableJSON:
        """
        Returns data for a particular entity type of single item.
        :param catalog: The name of the catalog to query
        :param entity_type: Which index to search (i.e. 'projects', 'specimens', etc.)
        :param pagination: A dictionary with pagination information as return from `_get_pagination()`
        :param filters: parsed JSON filters from the request
        :param item_id: If item_id is specified, only a single item is searched for
        :param file_url_func: A function that is used only when getting a *list* of files data.
        It creates the files URL based on info from the request. It should have the type
        signature `(uuid: str, **params) -> str`
        :return: The OpenSearch JSON response
        """
        if item_id is not None:
            validate_uuid(item_id)
            filters = filters.update({'entryId': {'is': [item_id]}})

        response = self._search(catalog=catalog,
                                filters=filters,
                                pagination=pagination,
                                aggregate=item_id is None,
                                entity_type=entity_type,
                                file_url_func=file_url_func)

        special_fields = self.metadata_plugin(catalog).special_fields
        for hit in response['hits']:
            entity = one(hit[entity_type])
            source_id = one(hit['sources'])[special_fields.source_id.name_in_hit]
            accessible = source_id in filters.source_ids
            entity[special_fields.accessible.name_in_hit] = accessible

        if item_id is not None:
            response = one(response['hits'], too_short=EntityNotFoundError(entity_type, item_id))
        return response

    def _search(self,
                *,
                catalog: CatalogName,
                entity_type: str,
                aggregate: bool,
                filters: Filters,
                pagination: Pagination,
                file_url_func: FileUrlFunc
                ) -> MutableJSON:
        """
        This function does the whole transformation process. It takes the path
        of the config file, the filters, and pagination, if any. Excluding
        filters will do a match_all request. Excluding pagination will exclude
        pagination from the output.

        :param catalog: The name of the catalog to query

        :param entity_type: the string referring to the entity type used to get
                            the OpenSearch index to search

        :param aggregate: Whether to perform the aggregation stage or not.

        :param filters: Filter parameter from the API to be used in the query.

        :param pagination: Pagination to be used for the API

        :return: Returns the transformed request
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
        try:
            response = request.execute(ignore_cache=True)
        except opensearchpy.NotFoundError as e:
            raise IndexNotFoundError(e.info['error']['index'])
        response = chain.process_response(response)
        return response

    def summary(self,
                catalog: CatalogName,
                filters: Filters
                ) -> MutableJSON:
        # FIXME: Due to the fact that we run multiple requests in parallel each
        #        in a separate chain, and the resulting need to multiplex the
        #        responses, the response stage is not part of any chain.
        #        https://github.com/DataBiosphere/azul/issues/4128
        plugin = self.metadata_plugin(catalog)
        response_stage = plugin.summary_response_stage()

        aggs_by_authority = response_stage.aggs_by_authority

        def summary(entity_type):
            return entity_type, self._summary(catalog=catalog,
                                              entity_type=entity_type,
                                              filters=filters)

        with ThreadPoolExecutor(max_workers=len(aggs_by_authority)) as executor:
            aggs = dict(executor.map(summary, aggs_by_authority))

        aggs = {
            agg_name: aggs[entity_type][agg_name]
            for entity_type, summary_fields in aggs_by_authority.items()
            for agg_name in summary_fields
        }

        response = response_stage.process_response(aggs)
        return response

    def _summary(self,
                 *,
                 catalog: CatalogName,
                 entity_type: str,
                 filters: Filters
                 ) -> MutableJSON:
        plugin = self.metadata_plugin(catalog)
        chain = self.create_chain(catalog=catalog,
                                  entity_type=entity_type,
                                  filters=filters,
                                  post_filter=False,
                                  document_slice=None)
        chain = ToDictStage(service=self,
                            catalog=catalog,
                            entity_type=entity_type).wrap(chain)
        chain = plugin.summary_aggregation_stage.create_and_wrap(chain)
        request = chain.prepare_request(self.create_request(catalog, entity_type))

        response = request.execute(ignore_cache=True)
        assert len(response.hits) == 0

        if config.debug == 2 and log.isEnabledFor(logging.DEBUG):
            log.debug('OpenSearch request: %s', json.dumps(request.to_dict(), indent=4))

        result = chain.process_response(response)

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
        filters = filters.update({
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
