from contextlib import (
    contextmanager,
)
import json
from pathlib import (
    Path,
)
from typing import (
    Any,
)
from unittest.mock import (
    PropertyMock,
    patch,
)

from furl import (
    furl,
)

from azul import (
    cached_property,
    config,
)
from azul.chalice import (
    AzulChaliceApp,
)
from azul.files import (
    write_file_atomically,
)
from azul.modules import (
    load_app_module,
)


def main():
    catalogs = {
        'dcp2': config.Catalog(name='dcp2',
                               atlas='hca',
                               internal=False,
                               plugins=dict(metadata=config.Catalog.Plugin(name='hca'),
                                            repository=config.Catalog.Plugin(name='tdr_hca')),
                               sources=set())
    }

    app_name = Path.cwd().name
    assert app_name in config.app_names(), app_name

    # To create a normalized OpenAPI document, we patch any
    # deployment-specific variables that affect the document.
    with (
        patch_config('catalogs', catalogs),
        patch_config(f'{app_name}_function_name', f'azul-{app_name}-dev'),
        patch_config('enable_log_forwarding', False),
        patch_config('enable_replicas', True),
        patch_config('monitoring_email', 'azul-group@ucsc.edu')
    ):
        lambda_endpoint = furl('http://localhost')
        with patch.object(target=AzulChaliceApp,
                          attribute='base_url',
                          new=lambda_endpoint):
            app_module = load_app_module(app_name)
            assert app_module.app.base_url == lambda_endpoint
            app_spec = app_module.app.spec()
            doc_path = Path(config.project_root) / 'lambdas' / app_name / 'openapi.json'
            with write_file_atomically(doc_path) as file:
                json.dump(app_spec, file, indent=4)


@contextmanager
def patch_config(attribute_name: str, value: Any):
    old_value = getattr(type(config), attribute_name)
    is_property = isinstance(old_value, (property, cached_property))
    with patch.object(target=type(config),
                      attribute=attribute_name,
                      new_callable=PropertyMock if is_property else None,
                      return_value=value):
        new_value = getattr(config, attribute_name)
        assert value == (new_value() if callable(old_value) else new_value)
        yield


if __name__ == '__main__':
    main()
