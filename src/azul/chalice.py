from chalice import Chalice

from azul.types import LambdaContext


class AzulChaliceApp(Chalice):
    def route(self, path, **kwargs):
        """
        Same as method in supper class but stashes URL path a view function is bound to as an attribute of the
        function itself.
        """
        decorator = super().route(path, **kwargs)

        def _decorator(view_func):
            view_func.path = path
            return decorator(view_func)

        return _decorator

    # A type annotation to help with auto-complete
    lambda_context: LambdaContext
