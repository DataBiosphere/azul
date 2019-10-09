from hca.util import SwaggerClient


class _Method:

    def _request(self): ...

    def __call__(self, *args, **kwargs): ...


class DSSClient(SwaggerClient):
    get_bundle = _Method()
    get_file = _Method()
    put_file = _Method()
    put_bundle = _Method()
    get_subscriptions = _Method()
    put_subscription = _Method()
    delete_subscription = _Method()
    post_search = _Method()
