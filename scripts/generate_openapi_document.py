import json
import os
from unittest.mock import (
    PropertyMock,
    patch,
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
                                            repository=config.Catalog.Plugin(name='tdr')),
                               sources=set())
    }

    # To create a normalized OpenAPI document, we patch any
    # deployment-specific variables that affect the document.
    with patch.object(target=type(config),
                      attribute='catalogs',
                      new_callable=PropertyMock,
                      return_value=catalogs):
        assert config.catalogs == catalogs
        with patch.object(target=config,
                          attribute='service_function_name',
                          return_value='azul_service'):
            assert config.service_name == 'azul_service'
            with patch.object(target=config,
                              attribute='service_endpoint',
                              return_value='http://localhost'):
                assert config.service_endpoint() == 'http://localhost'
                app_module = load_app_module('service')
                app_spec = app_module.app.spec()
                doc_path = os.path.join(config.project_root, 'lambdas/service/openapi.json')
                with write_file_atomically(doc_path) as file:
                    json.dump(app_spec, file, indent=4)


if __name__ == '__main__':
    main()
