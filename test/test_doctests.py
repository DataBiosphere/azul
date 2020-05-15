import doctest
import unittest

import azul
import azul.azulclient
import azul.collections
import azul.doctests
import azul.exceptions
import azul.dss
import azul.files
import azul.indexer.aggregate
import azul.json
import azul.json_freeze
from azul.logging import configure_test_logging
from azul.modules import (
    load_app_module,
    load_module,
)
import azul.objects
import azul.openapi
import azul.openapi.params
import azul.openapi.responses
import azul.openapi.schema
import azul.plugins.metadata.hca.full_metadata
import azul.service.elasticsearch_service
import azul.strings
import azul.threads
import azul.time
import azul.uuids
import azul.vendored.frozendict
import retorts


# noinspection PyPep8Naming
def setupModule():
    configure_test_logging()


def load_tests(_loader, tests, _ignore):
    root = azul.config.project_root
    for module in [azul,
                   azul.collections,
                   azul.doctests,
                   azul.dss,
                   azul.exceptions,
                   azul.files,
                   azul.json,
                   azul.json_freeze,
                   azul.objects,
                   azul.openapi,
                   azul.openapi.schema,
                   azul.openapi.params,
                   azul.openapi.responses,
                   azul.strings,
                   azul.threads,
                   azul.time,
                   azul.uuids,
                   azul.indexer.aggregate,
                   azul.vendored.frozendict,
                   azul.azulclient,
                   retorts,
                   azul.plugins.metadata.hca.full_metadata,
                   load_app_module('service'),
                   load_module(root + '/scripts/envhook.py', 'envhook'),
                   load_module(root + '/scripts/export_environment.py', 'export_environment'),
                   load_module(root + '/scripts/check_branch.py', 'check_branch'),
                   load_module(root + '/.flake8/azul_flake8.py', 'azul_flake8')]:
        suite = doctest.DocTestSuite(module)
        assert suite.countTestCases() > 0, module
        tests.addTests(suite)
    return tests


if __name__ == '__main__':
    unittest.main()
