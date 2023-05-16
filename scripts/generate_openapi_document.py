import json
from pathlib import (
    Path,
)
from unittest.mock import (
    PropertyMock,
    patch,
)

from furl import (
    furl,
)

from azul import (
    config,
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

    lambda_name = Path.cwd().name

    # To create a normalized OpenAPI document, we patch any
    # deployment-specific variables that affect the document.
    with patch.object(target=type(config),
                      attribute='catalogs',
                      new_callable=PropertyMock,
                      return_value=catalogs):
        assert config.catalogs == catalogs
        with patch.object(target=config,
                          attribute=f'{lambda_name}_function_name',
                          return_value=f'azul_{lambda_name}'):
            assert getattr(config, f'{lambda_name}_name') == f'azul_{lambda_name}'
            lambda_endpoint = furl('http://localhost')
            with patch.object(target=type(config),
                              attribute=f'{lambda_name}_endpoint',
                              new=lambda_endpoint):
                assert getattr(config, f'{lambda_name}_endpoint') == lambda_endpoint
                app_module = load_app_module(lambda_name)
                app_spec = app_module.app.spec()
                doc_path = Path(config.project_root) / 'lambdas' / lambda_name / 'openapi.json'
                with write_file_atomically(doc_path) as file:
                    json.dump(app_spec, file, indent=4)


if __name__ == '__main__':
    main()
