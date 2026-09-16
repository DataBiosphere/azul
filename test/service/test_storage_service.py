from datetime import (
    datetime,
    timedelta,
    timezone,
)
from functools import (
    partial,
)
from http.server import (
    BaseHTTPRequestHandler,
    ThreadingHTTPServer,
)
import tempfile
from threading import (
    Thread,
)
from unittest.mock import (
    patch,
)

import requests

from azul.http import (
    _max_atomic_read_size,
    http_client,
    read_large_http_response,
)
from azul.lib import (
    false,
)
from azul.lib.buffers import (
    BufferReader,
)
from azul.logging import (
    configure_test_logging,
)
from azul.service import (
    storage_service,
)
from azul.service.storage_service import (
    StorageObjectNotFound,
)
from service import (
    StorageServiceTestCase,
)


# noinspection PyPep8Naming
def setUpModule():
    configure_test_logging()


class StorageServiceTest(StorageServiceTestCase):
    """
    Functional Test for Storage Service
    """

    def test_upload_tags(self):
        object_key = 'test_file'
        with tempfile.NamedTemporaryFile('w') as f:
            f.write('some contents')
            f.flush()
            for tags in (None, {}, {'Name': 'foo', 'game': 'bar'}):
                with self.subTest(tags=tags):
                    self.storage_service.upload(file_path=f.name,
                                                object_key=object_key,
                                                tagging=tags)
                    if tags is None:
                        tags = {}
                    upload_tags = self.storage_service.get_object_tagging(object_key)
                    self.assertEqual(tags, upload_tags)

    def test_simple_get_put(self):
        sample_key = 'foo-simple'
        sample_content = b'bar'

        # NOTE: Ensure that the key does not exist before writing.
        with self.assertRaises(StorageObjectNotFound):
            self.storage_service.get_object(sample_key)

        self.storage_service.put_object(object_key=sample_key, data=sample_content)

        self.assertEqual(sample_content, self.storage_service.get_object(sample_key))

    def test_large_response_round_trip(self):
        """
        A response body read by :func:`read_large_http_response` must survive
        being passed to S3 through a :class:`BufferReader`, which is the route
        the mirror service takes. Both are typed loosely enough that only an
        actual request establishes that Boto3 accepts what it is given.
        """
        # Spans several reads, and is not a multiple of the read size
        size = 3 * _max_atomic_read_size + 1234
        body = (bytes(range(256)) * (size // 256 + 1))[:size]
        self.assertEqual(size, len(body))

        class Handler(BaseHTTPRequestHandler):

            def do_GET(self):
                self.send_response(200)
                self.send_header('Content-Type', 'application/octet-stream')
                self.send_header('Content-Length', str(size))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, format, *args):
                pass

        with ThreadingHTTPServer(('127.0.0.1', 0), Handler) as server:
            thread = Thread(target=partial(server.serve_forever, poll_interval=.1))
            thread.start()
            try:
                url = f'http://localhost:{server.server_port}'
                client = http_client()
                for object_key, upload in [('single', self._put_object),
                                           ('multipart', self._upload_parts)]:
                    with self.subTest(upload=object_key):
                        response = client.request('GET', url, preload_content=False)
                        buffer = read_large_http_response(response, size)
                        upload(object_key, buffer)
                        self.assertEqual(body, self.storage_service.get_object(object_key))
            finally:
                server.shutdown()
                thread.join()

    def _put_object(self, object_key, buffer):
        self.storage_service.put_object(object_key=object_key,
                                        data=BufferReader(buffer))

    def _upload_parts(self, object_key, buffer):
        upload_id = self.storage_service.create_multipart_upload(object_key=object_key)
        etag = self.storage_service.upload_multipart_part(object_key=object_key,
                                                          upload_id=upload_id,
                                                          part_number=1,
                                                          buffer=BufferReader(buffer))
        self.storage_service.complete_multipart_upload(object_key=object_key,
                                                       upload_id=upload_id,
                                                       etags=[etag])

    def test_simple_get_unknown_item(self):
        sample_key = 'foo-simple'

        with self.assertRaises(StorageObjectNotFound):
            self.storage_service.get_object(sample_key)

    def test_presigned_url(self):
        object_key, object_content = 'foo-presigned-url', b'{"a": 1}'
        service = self.storage_service
        service.put_object(object_key=object_key, data=object_content)
        for file_name in None, 'foo.json':
            with self.subTest(file_name=file_name):
                url = service.get_presigned_url(object_key, file_name=file_name)
                response = requests.get(url)
                if file_name is None:
                    self.assertNotIn('Content-Disposition', response.headers)
                else:
                    if false():
                        # Unfortunately, moto does not support emulating S3's
                        # mechanism of specifying response headers via request
                        # parameters (https://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectGET.html,
                        # section Request Parameters).
                        self.assertEqual(response.headers['Content-Disposition'],
                                         f'attachment;filename="{file_name}"')
                self.assertEqual(object_content, response.content)

    def test_time_until_object_expires(self):
        test_data = [(1, False), (0, False), (-1, True)]
        for object_age, expect_error in test_data:
            with self.subTest(object_age=object_age, expect_error=expect_error):
                with patch.object(storage_service, 'datetime') as mock_datetime:
                    now = datetime(2020, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
                    mock_datetime.now.return_value = now
                    expiration = 7
                    headers = {
                        'Expiration': 'expiry-date="Wed, 01 Jan 2020 00:00:00 UTC", rule-id="Test Rule"',
                        'LastModified': now - timedelta(days=float(expiration), seconds=object_age)
                    }
                    with patch.object(self.storage_service, 'head_object', return_value=headers):
                        with self.assertLogs(logger=storage_service.log, level='DEBUG') as logs:
                            actual = self.storage_service.time_until_object_expires('foo', expiration)
                            self.assertEqual(0, actual)
                        got_error = any('does not match' in log for log in logs.output)
                        self.assertIs(expect_error, got_error)
