import re
from typing import (
    Any,
    Callable,
)
from unittest import (
    mock,
)

from chalice import (
    Chalice,
    ChaliceUnhandledError,
    NotFoundError,
    Response,
)
import requests

from app_test_case import (
    LocalAppTestCase,
)
from azul_test_case import (
    DCP2TestCase,
)


class TestContentType(LocalAppTestCase, DCP2TestCase):

    @classmethod
    def lambda_name(cls) -> str:
        return 'service'

    @classmethod
    def setUpClass(cls):
        super().setUpClass()

        @cls.app_module.app.route('/test')
        def route():
            pass

    def _replace_handler(self, handler: Callable[[None], Any]):
        """
        Replace the current handler for route `/test` with the provided handler
        """
        route = '/test'
        app = self.__class__.app_module.app
        app.routes.pop(route)
        app._register_handler(handler_type='route',
                              name='route',
                              user_handler=handler,
                              wrapped_handler=handler,
                              kwargs={'path': route, 'kwargs': {}},
                              options=None)

    def _shrink_traceback(self, s: str) -> str:
        """
        Return a modified version of the given traceback. The first and last
        lines are kept, and everything inbetween is replaced with a single line
        of '...'.
        """
        if s.startswith('Traceback'):
            lines = s.split('\n')
            assert lines[-1] == '', s  # since traceback ends with a '\n'
            s = '\n'.join([lines[0], '...', lines[-2]])
        elif s.startswith('<html>'):
            # Assumes traceback is a json dumped string inside <pre></pre> tags
            pattern = re.compile(r'(<pre>)'
                                 r'("Traceback.*?\\n)'  # 1st line of traceback
                                 r'.*\\n'  # middle lines
                                 r'(.*)\\n'  # last line of traceback
                                 r'("</pre>)')
            s = re.sub(pattern, r'\1\2...\\n\3\4', s)
        return s

    def _test_route(self,
                    handler_fn: Callable[[None], Any],
                    expected_fn: Callable[[bool, bool], tuple[str, str]]
                    ):
        """
        Verify the response against expected for requests made with various
        types of `accept` header values.
        """
        self._replace_handler(handler_fn)
        for debug in (False, True):
            with mock.patch.object(Chalice, 'debug', 1 if debug else 0):
                for accept, expect_wrapped in [
                    (None, False),
                    ('*/*', False),
                    ('*/*,text/html', False),
                    ('text/html', True),
                    ('text/html,*/*', True),
                    ('*/*;q=0.9,text/html', True),
                    ('text/html;q=0.9,*/*;q=1.0', False),
                ]:
                    with self.subTest(debug=debug, accept=accept):
                        url = self.base_url.set(path=('test',))
                        headers = {'accept': accept}
                        response = requests.get(url, headers=headers)
                        response_text = self._shrink_traceback(response.text)
                        expected_text, expected_content_type = expected_fn(debug, expect_wrapped)
                        self.assertEqual(expected_text, response_text)
                        self.assertEqual(expected_content_type, response.headers['Content-Type'])

    def test_string(self):

        def route():
            return '<script />'

        def expected(_debug: bool, _wrapped: bool) -> tuple[str, str]:
            text = '<script />'
            content_type = 'application/json'
            return text, content_type

        self._test_route(route, expected)

    def test_json(self):

        def route():
            return {'<script />': '<iframe></iframe>'}

        def expected(_debug: bool, _wrapped: bool) -> tuple[str, str]:
            text = '{"<script />":"<iframe></iframe>"}'
            content_type = 'application/json'
            return text, content_type

        self._test_route(route, expected)

    def test_response_200(self):

        def route():
            return Response(status_code=200, body='<script />')

        def expected(_debug: bool, _wrapped: bool) -> tuple[str, str]:
            text = '<script />'
            content_type = 'application/json'
            return text, content_type

        self._test_route(route, expected)

    def test_response_200_text_plain(self):

        def route():
            return Response(status_code=200,
                            headers={'Content-Type': 'text/plain'},
                            body='<script />')

        def expected(_debug: bool, wrapped: bool) -> tuple[str, str]:
            if wrapped:
                text = (
                    '<html><body>'
                    '<h1>Status 200</h1>'
                    '<pre>"&lt;script /&gt;"</pre>'
                    '</body></html>'
                )
                content_type = 'text/html'
            else:
                text = '<script />'
                content_type = 'text/plain'
            return text, content_type

        self._test_route(route, expected)

    def test_response_200_text_html(self):

        def route():
            return Response(status_code=200,
                            headers={'Content-Type': 'text/html'},
                            body='<script />')

        def expected(_debug: bool, _wrapped: bool) -> tuple[str, str]:
            text = '<script />'
            content_type = 'text/html'
            return text, content_type

        self._test_route(route, expected)

    def test_NotFoundError(self):

        def route():
            raise NotFoundError('<script />')

        def expected(_debug: bool, _wrapped: bool) -> tuple[str, str]:
            text = '{"Code":"NotFoundError","Message":"<script />"}'
            content_type = 'application/json'
            return text, content_type

        self._test_route(route, expected)

    def test_ChaliceUnhandledError(self):

        def route():
            raise ChaliceUnhandledError('<script />')

        def expected(debug: bool, _wrapped: bool) -> tuple[str, str]:
            if debug:
                text = (
                    'Traceback (most recent call last):\n'
                    '...\n'
                    'chalice.app.ChaliceUnhandledError: <script />'
                )
                content_type = 'text/plain'
            else:
                text = (
                    '{"Code":"InternalServerError",'
                    '"Message":"An internal server error occurred."}'
                )
                content_type = 'application/json'
            return text, content_type

        self._test_route(route, expected)

    def test_exception(self):

        def route():
            raise Exception('<script />')

        def expected(debug: bool, wrapped: bool) -> tuple[str, str]:
            # Chalice's `_unhandled_exception_to_response` returns a
            # stacktrace if debug is enabled
            if debug:
                if wrapped:
                    text = (
                        '<html><body>'
                        '<h1>Status 500</h1>'
                        '<pre>"Traceback (most recent call last):\\n'
                        '...\\n'
                        'Exception: &lt;script /&gt;"</pre>'
                        '</body></html>'
                    )
                    content_type = 'text/html'
                else:
                    text = (
                        'Traceback (most recent call last):\n'
                        '...\n'
                        'Exception: <script />'
                    )
                    content_type = 'text/plain'
            else:
                text = (
                    '{"Code":"InternalServerError",'
                    '"Message":"An internal server error occurred."}'
                )
                content_type = 'application/json'
            return text, content_type

        self._test_route(route, expected)
