import hashlib
import json
from unittest.mock import (
    MagicMock,
    patch,
)

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
from azul.indexer.mirror_controller import (
    MirrorController,
)
from azul.indexer.mirror_service import (
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
from azul.types import (
    JSON,
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

    def test_mirroring(self):
        self._create_mock_queues(config.mirror_queue_names)
        with self.subTest('remote_mirror'):
            source_message = self._test_remote_mirror()

            with self.subTest('mirror_source'):
                partition_message = self._test_mirror_source(source_message)

                with self.subTest('mirror_partition'):
                    file, file_message = self._test_mirror_partition(partition_message)

                    with self.subTest('mirror_file', corrupted=False, exists=False):
                        self._test_mirror_file(file, file_message)

                    service = self.mirror_controller.service(self.catalog)
                    self._s3.delete_object(Bucket=self.mirror_bucket,
                                           Key=service.info_object_key(file))

                    with self.subTest('mirror_file', corrupted=True):
                        self._test_corrupted_download(file_message)

                    with self.subTest('mirror_file', corrupted=False, exists=True):
                        self._test_reuploaded_file(file_message)

    _file_contents = b'lorem ipsum dolor sit\n'

    @property
    def mirror_controller(self) -> MirrorController:
        return self.app_module.app.mirror_controller

    def _mirror_event(self, body: JSON) -> list[SQSRecord]:
        return [self._mock_sqs_record(body, fifo=True)]

    def _test_remote_mirror(self):
        self.client.remote_mirror(self.catalog, [self.source])
        source_message = one(self._read_queue(self.client.mirror_queue()))
        expected_message = dict(action='mirror_source',
                                catalog=self.catalog,
                                source=self.source.to_json())
        self.assertEqual(expected_message, source_message)
        return source_message

    def _test_mirror_source(self, source_message):
        event = self._mirror_event(source_message)
        self.mirror_controller.mirror(event)
        partition_messages = self._read_queue(self.client.mirror_queue())
        partition_message = copy_json(partition_messages[0])
        partitions = []
        for message in partition_messages:
            partitions.append(message.pop('prefix'))
            self.assertEqual(dict(action='mirror_partition',
                                  catalog=self.catalog,
                                  source=self.source.to_json()),
                             message)
        self.assertEqual(list(self.source.prefix.partition_prefixes()), partitions)
        return partition_message

    def _test_mirror_partition(self, partition_message):
        event = self._mirror_event(partition_message)
        file = HCAFile(uuid='405852c9-a0cc-4cd8-b9ff-7c6296223661',
                       name='foo.txt',
                       version=None,
                       drs_uri='drs://fake-domain.lan/foo',
                       size=len(self._file_contents),
                       content_type='text/plain',
                       sha256=hashlib.sha256(self._file_contents).hexdigest())
        plugin_cls = type(self.client.repository_plugin(self.catalog))
        with patch.object(plugin_cls, 'list_files', return_value=[file]):
            self.mirror_controller.mirror(event)
        file_message = one(self._read_queue(self.client.mirror_queue()))
        expected_message = dict(action='mirror_file',
                                catalog=self.catalog,
                                source=self.source.to_json(),
                                file=file.to_json())
        self.assertEqual(expected_message, file_message)
        return file, file_message

    def _test_mirror_file(self, file, file_message):
        event = self._mirror_event(file_message)
        with patch.object(MirrorService, '_download', return_value=self._file_contents):
            self.mirror_controller.mirror(event)
        service = self.mirror_controller.service(self.catalog)
        response = self._s3.get_object(Bucket=self.mirror_bucket,
                                       Key=service.mirror_object_key(file))
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

    def test_info_schema(self):
        client = http_client(log)
        file = MagicMock(content_type='text/plain')
        service = self.mirror_controller.service(self.catalog)
        info = service.info_object(file)
        response = client.request('GET', info['$schema'])
        self.assertEqual(200, response.status, response.data)
        schema = json.loads(response.data)
        jsonschema.validate(info, schema)
