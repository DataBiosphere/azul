import doctest
import unittest

import azul
import azul.azulclient
import azul.collections
import azul.dss
import azul.json
import azul.json_freeze
from azul.logging import configure_test_logging
from azul.modules import (
    load_module,
    load_app_module,
)
import azul.openapi
import azul.service.responseobjects.elastic_request_builder
import azul.strings
import azul.threads
import azul.time
import azul.transformer
import azul.vendored.frozendict
import retorts


# noinspection PyPep8Naming
def setupModule():
    configure_test_logging()


def load_tests(_loader, tests, _ignore):
    tests.addTests(doctest.DocTestSuite(azul))
    tests.addTests(doctest.DocTestSuite(azul.collections))
    tests.addTests(doctest.DocTestSuite(azul.dss))
    tests.addTests(doctest.DocTestSuite(azul.json))
    tests.addTests(doctest.DocTestSuite(azul.json_freeze))
    tests.addTests(doctest.DocTestSuite(azul.openapi))
    tests.addTests(doctest.DocTestSuite(azul.strings))
    tests.addTests(doctest.DocTestSuite(azul.threads))
    tests.addTests(doctest.DocTestSuite(azul.time))
    tests.addTests(doctest.DocTestSuite(azul.transformer))
    tests.addTests(doctest.DocTestSuite(azul.vendored.frozendict))
    tests.addTests(doctest.DocTestSuite(azul.azulclient))
    tests.addTests(doctest.DocTestSuite(azul.service.responseobjects.elastic_request_builder))
    tests.addTests(doctest.DocTestSuite(retorts))
    tests.addTests(doctest.DocTestSuite(load_app_module('service')))
    root = azul.config.project_root
    tests.addTests(doctest.DocTestSuite(load_module(root + '/scripts/envhook.py', 'envhook')))
    tests.addTests(doctest.DocTestSuite(load_module(root + '/scripts/check_branch.py', 'check_branch')))
    tests.addTests(doctest.DocTestSuite(load_module(root + '/scripts/copy_bundles.py', 'copy_bundles')))
    return tests


if __name__ == '__main__':
    unittest.main()
