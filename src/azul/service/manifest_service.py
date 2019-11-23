import logging
from typing import (
    List,
    Tuple,
)
import uuid

from azul import config
from azul.json_freeze import (
    freeze,
    sort_frozen,
)
from azul.plugin import (
    ManifestConfig,
)
from azul.service.elasticsearch_service import ElasticsearchService
from azul.service.hca_response_v5 import (
    ManifestResponse,
)
from azul.types import (
    JSON,
)

logger = logging.getLogger(__name__)


class ManifestService(ElasticsearchService):

    def transform_manifest(self, format_: str, filters: JSON):
        if format_ in ('tsv', 'compact'):
            manifest_config, source_filter, entity_type = self._manifest_params_tsv()
        elif format_ == 'full':
            manifest_config, source_filter, entity_type = self._manifest_params_full(filters)
        elif format_ in ('terra.bdbag', 'bdbag'):
            manifest_config, source_filter, entity_type = self._manifest_params_bdbag()
        else:
            assert False
        es_search = self._create_request(filters,
                                         post_filter=False,
                                         source_filter=source_filter,
                                         enable_aggregation=False,
                                         entity_type=entity_type)
        object_key = self._generate_manifest_object_key(filters) if format_ == 'full' else None
        manifest = ManifestResponse(self.plugin,
                                    manifest_config,
                                    es_search,
                                    format_,
                                    object_key=object_key)
        return manifest.return_response()

    def _generate_manifest_object_key(self, filters: JSON) -> str:
        """
        Generate and return a UUID string generated using the latest git commit and filters

        :param filters: Filter parameter eg. {'organ': {'is': ['Brain']}}
        :return: String representation of a UUID
        """
        git_commit = config.lambda_git_status['commit']
        manifest_namespace = uuid.UUID('ca1df635-b42c-4671-9322-b0a7209f0235')
        filter_string = repr(sort_frozen(freeze(filters)))
        return str(uuid.uuid5(manifest_namespace, git_commit + filter_string))

    def _manifest_params_tsv(self) -> Tuple[ManifestConfig, List[str], str]:
        manifest_config = self.service_config.manifest
        source_filter = self._default_source_filter(manifest_config)
        source_filter.append('contents.files.related_files')
        return manifest_config, source_filter, 'files'

    def _manifest_params_full(self, filters: JSON) -> Tuple[ManifestConfig, List[str], str]:
        source_filter = ['contents.metadata.*']
        entity_type = 'bundles'
        es_search = self._create_request(filters,
                                         post_filter=False,
                                         source_filter=source_filter,
                                         enable_aggregation=False,
                                         entity_type=entity_type)
        map_script = '''
                for (row in params._source.contents.metadata) {
                    for (f in row.keySet()) {
                        params._agg.fields.add(f);
                    }
                }
            '''
        reduce_script = '''
                Set fields = new HashSet();
                for (agg in params._aggs) {
                    fields.addAll(agg);
                }
                return new ArrayList(fields);
            '''
        es_search.aggs.metric('fields', 'scripted_metric',
                              init_script='params._agg.fields = new HashSet()',
                              map_script=map_script,
                              combine_script='return new ArrayList(params._agg.fields)',
                              reduce_script=reduce_script)
        es_search = es_search.extra(size=0)
        response = es_search.execute()
        assert len(response.hits) == 0
        aggregate = response.aggregations
        manifest_config = self._generate_full_manifest_config(aggregate)
        return manifest_config, source_filter, entity_type

    def _manifest_params_bdbag(self) -> Tuple[ManifestConfig, List[str], str]:
        # Terra rejects `.` in column names
        manifest_config = {
            path: {
                column_name.replace('.', ManifestResponse.column_path_separator): field_name
                for column_name, field_name in mapping.items()
            }
            for path, mapping in self.service_config.manifest.items()
        }
        return manifest_config, self._default_source_filter(manifest_config), 'files'

    def _default_source_filter(self, manifest_config):
        source_filter = [field_path_prefix + '.' + field_name
                         for field_path_prefix, field_mapping in manifest_config.items()
                         for field_name in field_mapping.values()]
        return source_filter

    def _generate_full_manifest_config(self, aggregate) -> JSON:
        manifest_config = {}
        for value in sorted(aggregate.fields.value):
            manifest_config[value] = value.split('.')[-1]
        return {'contents': manifest_config}
