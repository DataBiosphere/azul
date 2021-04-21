import responses


class ResponsesHelper:
    """
    Work around the lack of reentrance in the `responses` library. Both `moto`
    and `responses` suffer from an inherent design flaw: they use
    unittest.mock.patch which is global but their decorators and context
    managers pretend to be reentrant. It is essentially impossible to nest a
    RequestMock CM inside another one, or in a method decorated with
    @responses.activate or one of the moto decorators. Furthermore, one can't
    combine @responses.activate with any of the moto decorators. Use this
    method in tests as follows:

    >>> import moto
    >>> import requests
    >>> @moto.mock_sts
    ... def test_foo():
    ...     with ResponsesHelper() as helper:
    ...         helper.add(responses.Response(method=responses.GET,
    ...                                       url='http://foo.bar/blah',
    ...                                       body='Duh!'))
    ...         helper.add_passthru('http://localhost:12345/')
    ...         assert requests.get('http://foo.bar/blah').content == b'Duh!'
    >>> test_foo()

    >>> with ResponsesHelper() as helper: #doctest: +ELLIPSIS
    ...     pass
    Traceback (most recent call last):
    ...
    AssertionError: This helper only works with `responses` already active. ...

    In other words, whenever you would call the global responses.add() or
    responses.add_passthru() functions, call the helper's method of the same
    name instead. The helper will remove the mock response upon exit and
    restore the set pf pass-throughs to their original value, essentially
    undoiing all .add() and .add_passthru() invocations.

    Remember that you do not need @responses.activate if one of the moto
    decorators is present since they already activate responses.
    """

    def __init__(self, request_mock: responses.RequestsMock = None) -> None:
        super().__init__()
        # noinspection PyProtectedMember
        self.request_mock = responses._default_mock if request_mock is None else request_mock
        self.mock_responses = None
        self.passthru_prefixes = None

    def add(self, mock_response: responses.BaseResponse):
        self.request_mock.add(mock_response)
        self.mock_responses.append(mock_response)

    def add_passthru(self, prefix):
        self.request_mock.add_passthru(prefix)

    def __enter__(self):
        patcher = getattr(self.request_mock, '_patcher', None)
        # noinspection PyUnresolvedReferences,PyProtectedMember
        assert patcher is not None and hasattr(patcher, 'is_local'), (
            'This helper only works with `responses` already active. The '
            'easiest way to achieve that is to use the `@responses.activate` '
            'decorator or one or more of the moto decorators.'
        )
        self.mock_responses = []
        self.passthru_prefixes = self.request_mock.passthru_prefixes
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        for mock_response in self.mock_responses:
            self.request_mock.remove(mock_response)
        self.request_mock.passthru_prefixes = self.passthru_prefixes
