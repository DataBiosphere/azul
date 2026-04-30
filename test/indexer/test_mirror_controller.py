import hashlib
import json
from typing import (
    ContextManager,
)
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
    config,
)
from azul.deployment import (
    aws,
)
from azul.http import (
    http_client,
)
from azul.indexer.mirror_controller import (
    MirrorController,
)
from azul.indexer.mirror_service import (
    FilePart,
    MirrorAction,
    MirrorWorkerService,
)
from azul.lib import (
    R,
    cached_property,
)
from azul.lib.json import (
    copy_json,
)
from azul.lib.types import (
    JSON,
    MutableJSON,
    MutableJSONs,
)
from azul.logging import (
    configure_test_logging,
    get_test_logger,
)
from azul.plugins.metadata.hca import (
    HCAFile,
)
from azul.queues import (
    SQSFifoMessage,
)
from azul.service.source_service import (
    SourceService,
)
from azul.source import (
    SourceConfig,
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

    _operation_id = 'foo_op'

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.addClassPatch(patch.object(SourceService,
                                       'list_source_ids',
                                       return_value={cls.source.ref.id}))
        cls.addClassPatch(patch.object(MirrorAction,
                                       '_operation_id',
                                       return_value=cls._operation_id))

    _file_contents = b'lorem ipsum dolor sit\n'

    _file = HCAFile(uuid='405852c9-a0cc-4cd8-b9ff-7c6296223661',
                    name='foo.txt',
                    version=None,
                    drs_uri='drs://fake-domain.lan/foo',
                    size=len(_file_contents),
                    content_type='text/plain',
                    sha256=hashlib.sha256(_file_contents).hexdigest())

    def _mirror_file_message(self, file: HCAFile) -> MutableJSON:
        return dict(action='MirrorFileAction',
                    catalog=self.catalog,
                    operation_id=self._operation_id,
                    source=self.source.ref.to_json(),
                    prefix='00',
                    file=file.to_json())

    def _read_mirror_queue(self) -> MutableJSONs:
        return self._read_queue(self._service._mirror_queue())

    def _send_mirror_message(self, body: JSON):
        record = self._mock_sqs_record(body, fifo=True)
        message = SQSFifoMessage.from_record(record)
        self.queues.send_messages(self._service._mirror_queue(), [message])

    def _validate_file_contents(self, file: HCAFile, contents: bytes):
        response = self._s3.get_object(Bucket=self.mirror_bucket,
                                       Key=self._service._file_object_key(file))
        file_contents = response['Body'].read()
        self.assertEqual(file_contents, contents)

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
                                           Key=self._service._info_object_key(file))

                    with self.subTest('mirror_file (corrupted contents)'):
                        self._test_corrupted_download(file_message)

                    with self.subTest('mirror_file (exception on overwrite)'):
                        self._test_reuploaded_file(file_message)

    @property
    def _mirror_controller(self) -> MirrorController:
        return self._app.mirror_controller

    @property
    def _service(self) -> MirrorWorkerService:
        return self._mirror_controller.service(self.catalog)

    def _mirror_event(self, body: JSON) -> list[SQSRecord]:
        return [self._mock_sqs_record(body, fifo=True)]

    def _mirror_sources(self, *, enable=True) -> MutableJSONs:
        source_config = SourceConfig(mirror=enable)
        source = attrs.evolve(self.source, config=source_config)
        self._service.mirror_sources([source])
        return self._read_mirror_queue()

    def _patch_download(self, **kwargs) -> ContextManager:
        return patch.object(MirrorWorkerService, '_download', **kwargs)

    def _test_mirror_sources(self):
        source_message = one(self._mirror_sources())
        expected_message = dict(action='MirrorSourceAction',
                                catalog=self.catalog,
                                operation_id=self._operation_id,
                                source=self.source.ref.to_json())
        self.assertEqual(expected_message, source_message)
        return source_message

    def _test_mirror_source(self, source_message):
        event = self._mirror_event(source_message)
        self._mirror_controller.mirror(event)
        partition_messages = self._read_mirror_queue()
        partition_message = copy_json(partition_messages[0])
        partitions = []
        for message in partition_messages:
            partitions.append(message.pop('prefix'))
            self.assertEqual(dict(action='MirrorPartitionAction',
                                  catalog=self.catalog,
                                  operation_id=self._operation_id,
                                  source=self.source.ref.to_json()),
                             message)
        self.assertEqual(list(self.source.ref.prefix.partition_prefixes()), partitions)
        return partition_message

    def _test_mirror_partition(self, partition_message, files: list[HCAFile]):
        event = self._mirror_event(partition_message)
        plugin_cls = type(self._service.repository_plugin)
        with patch.object(plugin_cls, 'list_files', return_value=files):
            self._mirror_controller.mirror(event)
        file_message = one(self._read_mirror_queue())
        expected_message = self._mirror_file_message(self._file)
        self.assertEqual(expected_message, file_message)
        return file_message

    def _test_mirror_file(self, file, file_message):
        event = self._mirror_event(file_message)
        with self._patch_download(return_value=self._file_contents):
            self._mirror_controller.mirror(event)
        self._validate_file_contents(file, self._file_contents)
        content_types = self._get_content_types_from_info_object(file)
        self.assertEqual([file.content_type], content_types)

    def _test_corrupted_download(self, file_message):
        event = self._mirror_event(file_message)
        corrupted_contents = self._file_contents[:-1] + b'Q'
        with self._patch_download(return_value=corrupted_contents):
            with self.assertRaises(AssertionError) as e:
                self._mirror_controller.mirror(event)
            self.assertTrue(R.caused(e.exception))

    def _test_reuploaded_file(self, file_message):
        event = self._mirror_event(file_message)
        with self._patch_download(return_value=self._file_contents):
            with self.assertRaises(AssertionError) as e:
                self._mirror_controller.mirror(event)
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
            self._mirror_controller.mirror(event)
            new_content_types = self._get_content_types_from_info_object(file)
            if content_type in old_content_types:
                self.assertEqual(old_content_types, new_content_types)
            else:
                self.assertIn(content_type, new_content_types)

    @cached_property
    def _info_schema(self) -> JSON:
        version = self._service.info_schema_version
        schema = self._mirror_controller.get_schema('mirror', 'info', version)
        return schema

    def _get_content_types_from_info_object(self, file) -> list[str]:
        service = self._service
        info = json.loads(service._storage.get_object(service._info_object_key(file)))
        jsonschema.validate(info, self._info_schema)
        content_types = info['content-type']
        self.assertIsInstance(content_types, list)
        self.assertEqual(sorted(set(content_types)), content_types)
        return content_types

    def test_info_schema_response(self):
        client = http_client(log)
        file = MagicMock(content_type='text/plain')
        info = self._service._info(file)
        schema_url = info['$schema']
        response = client.request('GET', schema_url)
        self.assertEqual(200, response.status, response.data)
        schema = json.loads(response.data)
        self.assertEqual(self._info_schema, schema)
        jsonschema.validate(info, schema)

    def test_info_schema(self):
        schema = self._info_schema
        instance = {
            'content-type': ['application/binary'],
            '$schema': 'https://localhost/schemas/mirror/info/v2.json'
        }
        jsonschema.validate(instance, schema)
        invalid_instances = [
            {
                'content-type': ['application/binary'],
                '$schema': 'https://localhost/schemas/mirror/info/v0.json'
            },
            {
                'content-type': 'application/binary',
                '$schema': 'https://localhost/schemas/mirror/info/v3.json'
            },
            {
                '$schema': 'https://localhost/schemas/mirror/info/v4.json'
            }
        ]
        for instance in invalid_instances:
            with self.assertRaises(jsonschema.exceptions.ValidationError):
                jsonschema.validate(instance, schema)

    def test_files_not_mirrored(self):
        self._create_mock_queues(config.mirror_queue_names)

        with self.subTest(no_mirror=True):
            messages = self._mirror_sources(enable=False)
            self.assertEqual([], messages)

        with self.subTest(mirror_limit=-1):
            with self._patch_mirror_limit(self.catalog, -1):
                messages = self._mirror_sources()
                self.assertEqual([], messages)

        with self.subTest(mirror_limit=self._file.size):
            too_big = attrs.evolve(self._file,
                                   uuid='2873c8ef-8f76-4ccf-add7-26afe8c62873',
                                   size=self._file.size + 1)
            source_message = self._test_mirror_sources()
            partition_message = self._test_mirror_source(source_message)
            with self._patch_mirror_limit(self.catalog, self._file.size):
                self._test_mirror_partition(partition_message, [too_big, self._file])

    def test_multi_part_upload(self):
        self._create_mock_queues(config.mirror_queue_names)
        min_size = aws.s3_min_part_size
        file_size = min_size + 1

        big_contents = self._file_contents + (b'0' * (file_size - len(self._file_contents)))
        assert len(big_contents) == file_size
        big_file = attrs.evolve(self._file,
                                size=file_size,
                                sha256=hashlib.sha256(big_contents).hexdigest())

        def download(_self, _file, part: FilePart | None = None) -> bytes:
            return big_contents[part.offset:part.offset + part.size]

        # Skip over mirror_source and mirror_partition to keep things simple
        self._send_mirror_message(self._mirror_file_message(big_file))
        with patch.object(FilePart, 'default_size', new=min_size):
            with self._patch_download(new=download):
                for action in ['MirrorFileAction', 'MirrorPartAction', 'FinalizeFileAction']:
                    message = one(self._read_mirror_queue())
                    event = self._mirror_event(message)
                    self.assertEqual(action, json.loads(one(event).body)['action'])
                    self._mirror_controller.mirror(event)
        self._validate_file_contents(big_file, big_contents)
