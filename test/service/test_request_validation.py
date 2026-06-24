import json

from furl import (
    furl,
)
import urllib3

from azul.logging import (
    configure_test_logging,
    get_test_logger,
)
from azul.plugins import (
    MetadataPlugin,
)
from azul.service.index_controller import (
    IndexController,
)
from indexer import (
    DCP1CannedBundleTestCase,
)
from service import (
    WebServiceTestCase,
)


# noinspection PyPep8Naming
def setUpModule():
    configure_test_logging()


log = get_test_logger(__name__)


class RequestParameterValidationTest(DCP1CannedBundleTestCase,
                                     WebServiceTestCase):
    maxDiff = None

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls._setup_indices()

    @classmethod
    def tearDownClass(cls):
        cls._teardown_indices()
        super().tearDownClass()

    @property
    def _controller(self) -> IndexController:
        controller = self._app.index_controller
        assert isinstance(controller, IndexController)
        return controller

    @property
    def _metadata_plugin(self) -> MetadataPlugin:
        controller = self._controller
        plugin = controller._metadata_plugin
        assert isinstance(plugin, MetadataPlugin)
        return plugin

    def assertResponseStatus(self,
                             url: furl,
                             status: int
                             ) -> urllib3.BaseHTTPResponse:
        if str(url.path) in {'/manifest/files', '/fetch/manifest/files'}:
            method = 'PUT'
        else:
            method = 'GET'
        response = self._http_client.request(method, str(url))
        self.assertEqual(status, response.status, response.data)
        return response

    def assertErrorMessage(self, url: furl, status: int, code: str, message: str):
        response = self.assertResponseStatus(url, status)
        expected_response = {
            'Code': code,
            'Message': message
        }
        self.assertEqual(expected_response, response.json())

    def assertBadRequest(self, url: furl, message: str):
        self.assertErrorMessage(url, 400, 'BadRequestError', message)

    def assertNotFound(self, url: furl, message: str):
        self.assertErrorMessage(url, 404, 'NotFoundError', message)

    def assertBadField(self, url: furl):
        self.assertBadRequest(url, 'Unknown field `bad-field`')

    def assertBadFilterField(self, url: furl):
        self.assertBadRequest(url,
                              "The value of the `filters` parameter is invalid against the schema: "
                              "Additional properties are not allowed "
                              "('bad-field' was unexpected) at path $")

    def assertBadFilterFields(self, url: furl):
        self.assertBadRequest(url,
                              "The value of the `filters` parameter is invalid against the schema: "
                              "Additional properties are not allowed "
                              "('bad-field', 'bad-field2' were unexpected) at path $")

    def test_bad_single_filter_field_of_sample(self):
        params = {
            'catalog': self.catalog,
            'size': 1,
            'filters': json.dumps({'bad-field': {'is': ['fake-val']}}),
        }
        url = self.base_url.set(path='/index/samples', args=params)
        self.assertBadFilterField(url)

    def test_bad_multiple_filter_field_of_sample(self):
        params = {
            'catalog': self.catalog,
            'size': 1,
            'filters': json.dumps({'bad-field': {'is': ['fake-val']}, 'bad-field2': {'is': ['fake-val2']}}),
        }
        url = self.base_url.set(path='/index/samples', args=params)
        self.assertBadFilterFields(url)

    def test_mixed_multiple_filter_field_of_sample(self):
        params = {
            'catalog': self.catalog,
            'size': 1,
            'filters': json.dumps({'organPart': {'is': ['fake-val']}, 'bad-field': {'is': ['fake-val']}}),
        }
        url = self.base_url.set(path='/index/samples', args=params)
        self.assertBadFilterField(url)

    def test_bad_sort_field_of_sample(self):
        params = {
            'size': 1,
            'filters': json.dumps({}),
            'sort': 'bad-field',
            'order': 'asc',
        }
        url = self.base_url.set(path='/index/samples', args=params)
        self.assertBadField(url)

    def test_bad_sort_field_and_filter_field_of_sample(self):
        params = {
            'size': 15,
            'filters': json.dumps({'bad-field': {'is': ['fake-val']}}),
            'sort': 'bad-field',
            'order': 'asc',
        }
        url = self.base_url.set(path='/index/samples', args=params)
        self.assertBadFilterField(url)

    def test_valid_sort_field_but_bad_filter_field_of_sample(self):
        params = {
            'catalog': self.catalog,
            'size': 15,
            'filters': json.dumps({'bad-field': {'is': ['fake-val']}}),
            'sort': 'organPart',
            'order': 'asc',
        }
        url = self.base_url.set(path='/index/samples', args=params)
        self.assertBadFilterField(url)

    def test_bad_sort_field_but_valid_filter_field_of_sample(self):
        params = {
            'size': 15,
            'filters': json.dumps({'organPart': {'is': ['fake-val2']}}),
            'sort': 'bad-field',
            'order': 'asc',
        }
        url = self.base_url.set(path='/index/samples', args=params)
        self.assertBadField(url)

    def test_bad_single_filter_field_of_file(self):
        params = {
            'catalog': self.catalog,
            'size': 1,
            'filters': json.dumps({'bad-field': {'is': ['fake-val2']}}),
        }
        url = self.base_url.set(path='/index/files', args=params)
        self.assertBadFilterField(url)

    def test_bad_multiple_filter_field_of_file(self):
        params = {
            'catalog': self.catalog,
            'size': 1,
            'filters': json.dumps({'bad-field': {'is': ['fake-val']}, 'bad-field2': {'is': ['fake-val2']}}),
        }
        url = self.base_url.set(path='/index/files', args=params)
        self.assertBadFilterFields(url)

    def test_mixed_multiple_filter_field_of_file(self):
        params = {
            'catalog': self.catalog,
            'size': 1,
            'filters': json.dumps({'organPart': {'is': ['fake-val']}, 'bad-field': {'is': ['fake-val']}}),
        }
        url = self.base_url.set(path='/index/files', args=params)
        self.assertBadFilterField(url)

    def test_source_filter(self):
        special_fields = self._metadata_plugin.special_fields
        for field, expected_type in [
            (special_fields.source_id, 'string'),
            (special_fields.accessible, 'boolean')
        ]:
            with self.subTest(field=field):
                params = {
                    'catalog': self.catalog,
                    'size': 1,
                    'filters': json.dumps({field.name: {'is': [None]}})
                }
                url = self.base_url.set(path='/index/projects', args=params)
                error = (f"The value of the `filters` parameter is invalid against the schema: "
                         f"None is not of type '{expected_type}' at path $.{field.name}.is[0]")
                self.assertBadRequest(url, error)

    def test_bad_sort_field_of_file(self):
        params = {
            'size': 15,
            'sort': 'bad-field',
            'order': 'asc',
            'filters': json.dumps({}),
        }
        url = self.base_url.set(path='/index/files', args=params)
        self.assertBadField(url)

    def test_bad_sort_field_and_filter_field_of_file(self):
        params = {
            'catalog': self.catalog,
            'size': 15,
            'filters': json.dumps({'bad-field': {'is': ['fake-val2']}}),
        }
        url = self.base_url.set(path='/index/files', args=params)
        self.assertBadFilterField(url)

    def test_bad_sort_field_but_valid_filter_field_of_file(self):
        params = {
            'size': 15,
            'sort': 'bad-field',
            'order': 'asc',
            'filters': json.dumps({'organ': {'is': ['fake-val2']}}),
        }
        url = self.base_url.set(path='/index/files', args=params)
        self.assertBadField(url)

    def test_valid_sort_field_but_bad_filter_field_of_file(self):
        params = {
            'catalog': self.catalog,
            'size': 15,
            'sort': 'organPart',
            'order': 'asc',
            'filters': json.dumps({'bad-field': {'is': ['fake-val2']}}),
        }
        url = self.base_url.set(path='/index/files', args=params)
        self.assertBadFilterField(url)

    def test_bad_filters(self):
        url = self.base_url.set(path='/index/files', args=dict(catalog=self.catalog))
        schema_error = "The value of the `filters` parameter is invalid against the schema: "
        cases = [
            (
                '"',
                "The 'filters' parameter is not valid JSON"
            ),
            (
                '""',
                schema_error + "'' is not of type 'object' at path $"
            ),
            (
                '{"sampleDisease": ["H syndrome"]}',
                schema_error + "['H syndrome'] is not of type 'object' at path $.sampleDisease"
            ),
            (
                '{"sampleDisease": {"is": "H syndrome"}}',
                schema_error + "'H syndrome' is not of type 'array' at path $.sampleDisease.is"
            ),
            (
                '{"sampleDisease": {"was": "H syndrome"}}',
                schema_error + "'is' is a required property at path $.sampleDisease"
            ),
            (
                '{"fileSource": {"is": [["foo:23/33"]]}}',
                schema_error + "['foo:23/33'] is not of type 'null', 'string' "
                               "at path $.fileSource.is[0]"
            ),
            (
                '{"accessions": {"within": ["foo"]}}',
                schema_error + "'is' is a required property at path $.accessions"
            ),
            (
                '{"accessions": {"is": []}}',
                schema_error + "[] should be non-empty at path $.accessions.is"
            ),
            (
                '{"accessions": {"is": ["foo"]}}',
                schema_error + "'foo' is not of type 'object' at path $.accessions.is[0]"
            ),
            (
                '{"accessions": {"is": [{"foo": "geostudies"}]}}',
                schema_error + "Additional properties are not allowed "
                               "('foo' was unexpected) at path $.accessions.is[0]"
            ),
            (
                '{"accessions": {"is": [{"namespace": "baz", "foo": "bar"}]}}',
                schema_error + "Additional properties are not allowed "
                               "('foo' was unexpected) at path $.accessions.is[0]"
            ),
            (
                json.dumps({'accessions': {'is': [{'namespace': 'x', 'accession': 'y'}] * 2}}),
                schema_error + "[{'namespace': 'x', 'accession': 'y'}, "
                               "{'namespace': 'x', 'accession': 'y'}] "
                               "is too long at path $.accessions.is"
            ),
            (
                '{"projectTitle":{"contains":["retina"]}}',
                schema_error + "'is' is a required property at path $.projectTitle"
            ),
            (
                '{"assayType":{"is":["flow cytometry"]}}',
                schema_error + "'flow cytometry' is not of type 'object' at path $.assayType.is[0]"
            ),
            (
                '{"organismAge":{"is":[]}}',
                schema_error + "[] should be non-empty at path $.organismAge.is"
            ),
            (
                '{"organismAge":{"is":[""]}}',
                schema_error + "'' is not valid under any of the given schemas "
                               "at path $.organismAge.is[0]"
            ),
            (
                '{"organismAge":{"contains":[{"value": "1", "unit": "year"}]}}',
                schema_error + "'is' is a required property at path $.organismAge"
            ),
            (
                '{"organismAge":{"is":[{}]}}',
                schema_error + "{} is not valid under any of the given schemas "
                               "at path $.organismAge.is[0]"
            ),
            (
                '{"organismAge":{"is":[{"value": "1"}]}}',
                schema_error + "{'value': '1'} is not valid under any of the given schemas "
                               "at path $.organismAge.is[0]"
            ),
            (
                '{"organismAge":{"is":[{"value": "1", "unit": "year", "foo": "year"}]}}',
                schema_error + "{'value': '1', 'unit': 'year', 'foo': 'year'} "
                               "is not valid under any of the given schemas "
                               "at path $.organismAge.is[0]"
            ),
            (
                '{"organismAge":{"is":[{"value": "1", "unit": "year"}, {}]}}',
                schema_error + "{} is not valid under any of the given schemas "
                               "at path $.organismAge.is[1]"
            )
        ]
        for filters, message in cases:
            with self.subTest(filters=filters):
                url.args.set('filters', filters)
                self.assertBadRequest(url, message)

    def test_single_entity_error_responses(self):
        entity_types = ['files', 'projects']
        for uuid, expected_error_code in [('2b7959bb-acd1-4aa3-9557-345f9b3c6327', 404),
                                          ('-0c5ac7c0-817e-40d4-b1b1-34c3d5cfecdb-', 400),
                                          ('FOO', 400)]:
            for entity_type in entity_types:
                with self.subTest(entity_name=entity_type, error_code=expected_error_code, uuid=uuid):
                    url = self.base_url.set(path=('index', entity_type, uuid))
                    self.assertResponseStatus(url, expected_error_code)

    def test_bad_query_params(self):

        for entity_type in ('files', 'bundles', 'samples'):
            url = self.base_url.set(path=('index', entity_type))
            with self.subTest(entity_type=entity_type):
                url.args = dict(catalog=self.catalog,
                                some_nonexistent_filter=1)
                self.assertBadRequest(url, 'Unknown query parameter `some_nonexistent_filter`')

    def test_bad_catalog_param(self):
        for path in (*('/index/' + e for e in ('summary', 'files')),
                     '/manifest/files',
                     '/repository/files/74897eb7-0701-4e4f-9e6b-8b9521b2816b'):
            for catalog, test, message in [
                ('foo', self.assertNotFound, "Catalog name 'foo' does not exist. Must be one of %r." % {self.catalog}),
                ('foo ', self.assertBadRequest, "('Catalog name is invalid', 'foo ')")
            ]:
                with self.subTest(path=path, catalog=catalog):
                    url = self.base_url.set(path=path, args=dict(catalog=catalog))
                    test(url, message)

    def test_bad_entity_type(self):
        bad_entity_type = 'spiders'
        good_entity_types = set(self._metadata_plugin.exposed_indices)
        assert bad_entity_type not in good_entity_types
        url = self.base_url.set(path='/index/' + bad_entity_type)
        expected = (f'Entity type {bad_entity_type!r} is invalid for catalog '
                    f'{self.catalog!r}. Must be one of {good_entity_types}.')
        self.assertBadRequest(url, expected)

    def test_bad_manifest_format(self):
        bad_format = 'fluffy'
        good_formats = {f.value for f in self._metadata_plugin.manifest_formats}
        assert bad_format not in good_formats
        url = self.base_url.set(path='/manifest/files',
                                query_params={'format': bad_format})
        expected = (f'Unknown manifest format `{bad_format}`. '
                    f'Must be one of {good_formats}')
        self.assertBadRequest(url, expected)

    def test_size(self):
        url = self.base_url.set(path='/index/files')
        for size, test, arg in [
            (1001, self.assertBadRequest, 'Invalid value for parameter `size`, must not be greater than 1000'),
            (0, self.assertBadRequest, 'Invalid value for parameter `size`, must be greater than 0'),
            ('foo', self.assertBadRequest, 'Invalid value for parameter `size`')
        ]:
            with self.subTest(size=size):
                url.args.set('size', size)
                test(url, arg)

    def test_order(self):
        url = self.base_url.set(path='/index/projects')
        for order, arg in [
            ('foo', "Unknown order `foo`. Must be one of ('asc', 'desc')"),
            ('asc', None),
            ('desc', None)
        ]:
            with self.subTest(order=order):
                url.args.set('order', order)
                if arg:
                    self.assertBadRequest(url, arg)
                else:
                    self.assertResponseStatus(url, 200)

    def test_version(self):
        for fetch in [False, True]:
            for file_id, version, error in [
                ('74897eb7-0701-4e4f-9e6b-8b9521b2816b', 'foo', 'Invalid value for `version`'),
                ('foo', '2018-11-02T11:33:44.450442Z', 404)
            ]:
                with self.subTest(fetch=fetch, file_id=file_id, version=version, error=error):
                    url = self.base_url.set(path=f'repository/files/{file_id}',
                                            query_params={'version': version})
                    if fetch:
                        url.path.segments.insert(0, 'fetch')
                    if isinstance(error, int):
                        self.assertResponseStatus(url, error)
                    else:
                        self.assertBadRequest(url, error)
