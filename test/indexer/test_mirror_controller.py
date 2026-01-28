import hashlib
import json
from unittest.mock import (
    MagicMock,
    patch,
)

import attrs
from chalice.app import (
    SQSRecord,
)
import jsonschema
from more_itertools import (
    one,
)

from app_test_case import (
    LocalAppTestCase,
)
from azul import (
    R,
    config,
)
from azul.http import (
    http_client,
)
from azul.indexer import (
    SourceConfig,
)
from azul.indexer.mirror_controller import (
    MirrorController,
)
from azul.indexer.mirror_service import (
    MirrorAction,
    MirrorService,
)
from azul.json import (
    copy_json,
)
from azul.logging import (
    configure_test_logging,
    get_test_logger,
)
from azul.plugins.metadata.hca import (
    HCAFile,
)
from azul.service.source_service import (
    SourceService,
)
from azul.types import (
    JSON,
    MutableJSONs,
)
from azul_test_case import (
    DCP2TestCase,
)
from service import (
    MirrorTestCase,
)
from sqs_test_case import (
    WorkQueueTestCase,
)

log = get_test_logger(__name__)


# noinspection PyPep8Naming
def setUpModule():
    configure_test_logging(log)


class TestMirrorController(DCP2TestCase,
                           LocalAppTestCase,
                           WorkQueueTestCase,
                           MirrorTestCase):

    @classmethod
    def app_name(cls) -> str:
        return 'indexer'

    operation_id = 'foo_op'

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.addClassPatch(patch.object(SourceService,
                                       'list_accessible_source_ids',
                                       return_value={cls.source.id}))
        cls.addClassPatch(patch.object(MirrorAction,
                                       '_operation_id',
                                       return_value=cls.operation_id))

    _file_contents = b'lorem ipsum dolor sit\n'

    _file = HCAFile(uuid='405852c9-a0cc-4cd8-b9ff-7c6296223661',
                    name='foo.txt',
                    version=None,
                    drs_uri='drs://fake-domain.lan/foo',
                    size=len(_file_contents),
                    content_type='text/plain',
                    sha256=hashlib.sha256(_file_contents).hexdigest())

    def test_mirroring(self):
        self._create_mock_queues(config.mirror_queue_names)
        file = self._file
        with self.subTest('mirror_sources'):
            source_message = self._test_mirror_sources()

            with self.subTest('mirror_source'):
                partition_message = self._test_mirror_source(source_message)

                with self.subTest('mirror_partition'):
                    file_message = self._test_mirror_partition(partition_message, [file])

                    with self.subTest('mirror_file (fresh upload)'):
                        self._test_mirror_file(file, file_message)

                    with self.subTest('mirror_file (update existing info)'):
                        self._test_content_type_update(file, file_message)

                    self._s3.delete_object(Bucket=self.mirror_bucket,
                                           Key=self.service._info_object_key(file))

                    with self.subTest('mirror_file (corrupted contents)'):
                        self._test_corrupted_download(file_message)

                    with self.subTest('mirror_file (exception on overwrite)'):
                        self._test_reuploaded_file(file_message)

    @property
    def mirror_controller(self) -> MirrorController:
        return self._app.mirror_controller

    @property
    def service(self) -> MirrorService:
        return self.mirror_controller.service(self.catalog)

    def _mirror_event(self, body: JSON) -> list[SQSRecord]:
        return [self._mock_sqs_record(body, fifo=True)]

    def _mirror_sources(self, source_config=SourceConfig(mirror=True)) -> MutableJSONs:
        self.service.mirror_sources([(self.source, source_config)])
        return self._read_queue(self.service._mirror_queue())

    def _test_mirror_sources(self):
        source_message = one(self._mirror_sources())
        expected_message = dict(action='MirrorSourceAction',
                                catalog=self.catalog,
                                operation_id=self.operation_id,
                                source=self.source.to_json())
        self.assertEqual(expected_message, source_message)
        return source_message

    def _test_mirror_source(self, source_message):
        event = self._mirror_event(source_message)
        self.mirror_controller.mirror(event)
        partition_messages = self._read_queue(self.service._mirror_queue())
        partition_message = copy_json(partition_messages[0])
        partitions = []
        for message in partition_messages:
            partitions.append(message.pop('prefix'))
            self.assertEqual(dict(action='MirrorPartitionAction',
                                  catalog=self.catalog,
                                  operation_id=self.operation_id,
                                  source=self.source.to_json()),
                             message)
        self.assertEqual(list(self.source.prefix.partition_prefixes()), partitions)
        return partition_message

    def _test_mirror_partition(self, partition_message, files: list[HCAFile]):
        event = self._mirror_event(partition_message)
        plugin_cls = type(self.service._repository_plugin)
        with patch.object(plugin_cls, 'list_files', return_value=files):
            self.mirror_controller.mirror(event)
        file_message = one(self._read_queue(self.service._mirror_queue()))
        expected_message = dict(action='MirrorFileAction',
                                catalog=self.catalog,
                                operation_id=self.operation_id,
                                source=self.source.to_json(),
                                prefix='00',
                                file=self._file.to_json())
        self.assertEqual(expected_message, file_message)
        return file_message

    def _test_mirror_file(self, file, file_message):
        event = self._mirror_event(file_message)
        with patch.object(MirrorService, '_download', return_value=self._file_contents):
            self.mirror_controller.mirror(event)
        response = self._s3.get_object(Bucket=self.mirror_bucket,
                                       Key=self.service._file_object_key(file))
        mirrored_file_contents = response['Body'].read()
        self.assertEqual(mirrored_file_contents, self._file_contents)

    def _test_corrupted_download(self, file_message):
        event = self._mirror_event(file_message)
        corrupted_contents = self._file_contents[:-1] + b'Q'
        with patch.object(MirrorService, '_download', return_value=corrupted_contents):
            with self.assertRaises(AssertionError) as e:
                self.mirror_controller.mirror(event)
            self.assertTrue(R.caused(e.exception))

    def _test_reuploaded_file(self, file_message):
        event = self._mirror_event(file_message)
        with patch.object(MirrorService, '_download', return_value=self._file_contents):
            with self.assertRaises(AssertionError) as e:
                self.mirror_controller.mirror(event)
        self.assertTrue(R.caused(e.exception))
        self.assertEqual(e.exception.args[0].args[0], 'File object is already present')

    def _test_content_type_update(self, file, file_message):
        for content_type in [
            'application/octet-stream',
            'application/octet-stream',
            'text/csv; charset="utf-8"',
            'application/octet-stream',
            'text/plain',
        ]:
            changed_message = {
                **file_message,
                'file': attrs.evolve(file, content_type=content_type).to_json()
            }
            old_content_types = self._get_content_types_from_info_object(file)
            event = self._mirror_event(changed_message)
            self.mirror_controller.mirror(event)
            new_content_types = self._get_content_types_from_info_object(file)
            if content_type in old_content_types:
                self.assertEqual(old_content_types, new_content_types)
            else:
                self.assertIn(content_type, new_content_types)

    def _get_content_types_from_info_object(self, file) -> list[str]:
        service = self.service
        info = json.loads(service._storage.get_object(service._info_object_key(file)))
        content_types = info['content-type']
        self.assertIsInstance(content_types, list)
        self.assertEqual(sorted(set(content_types)), content_types)
        return content_types

    def test_info_schema(self):
        client = http_client(log)
        file = MagicMock(content_type='text/plain')
        info = self.service._info(file)
        response = client.request('GET', info['$schema'])
        self.assertEqual(200, response.status, response.data)
        schema = json.loads(response.data)
        jsonschema.validate(info, schema)

    def test_files_not_mirrored(self):
        self._create_mock_queues(config.mirror_queue_names)

        with self.subTest(no_mirror=True):
            messages = self._mirror_sources(SourceConfig(mirror=False))
            self.assertEqual([], messages)

        catalog = config.catalogs[self.catalog]

        def patch_mirror_limit(size):
            return patch.dict(config.catalogs, {
                self.catalog: attrs.evolve(catalog, mirror_limit=size)
            })

        with self.subTest(mirror_limit=-1):
            with patch_mirror_limit(-1):
                messages = self._mirror_sources()
                self.assertEqual([], messages)

        with self.subTest(mirror_limit=self._file.size):
            too_big = attrs.evolve(self._file,
                                   uuid='2873c8ef-8f76-4ccf-add7-26afe8c62873',
                                   size=self._file.size + 1)
            source_message = self._test_mirror_sources()
            partition_message = self._test_mirror_source(source_message)
            with patch_mirror_limit(self._file.size):
                self._test_mirror_partition(partition_message, [too_big, self._file])
