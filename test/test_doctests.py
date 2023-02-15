import doctest
import unittest

import azul
import azul.auth
import azul.azulclient
import azul.bigquery
import azul.caching
import azul.collections
import azul.doctests
import azul.dss
import azul.exceptions
import azul.files
import azul.indexer
import azul.indexer.aggregate
import azul.indexer.document
import azul.iterators
import azul.json
import azul.json_freeze
from azul.logging import (
    configure_test_logging,
)
from azul.modules import (
    load_app_module,
    load_module,
    load_script,
)
import azul.objects
import azul.openapi
import azul.openapi.params
import azul.openapi.responses
import azul.openapi.schema
import azul.plugins.metadata.hca.indexer.transform
import azul.plugins.metadata.hca.service.contributor_matrices
import azul.plugins.repository.tdr_hca
import azul.service.drs_controller
import azul.service.manifest_service
import azul.service.repository_controller
import azul.strings
import azul.terra
import azul.terraform
import azul.threads
import azul.time
import azul.types
import azul.uuids
import azul.vendored.frozendict
import service
import test_tagging


# noinspection PyPep8Naming
def setUpModule():
    configure_test_logging()


def load_tests(_loader, tests, _ignore):
    root = azul.config.project_root
    for module in [
        azul,
        azul.auth,
        azul.azulclient,
        azul.bigquery,
        azul.caching,
        azul.collections,
        azul.doctests,
        azul.dss,
        azul.exceptions,
        azul.files,
        azul.indexer,
        azul.indexer.aggregate,
        azul.indexer.document,
        azul.iterators,
        azul.json,
        azul.json_freeze,
        azul.objects,
        azul.openapi,
        azul.openapi.params,
        azul.openapi.responses,
        azul.openapi.schema,
        azul.plugins.metadata.hca.service.contributor_matrices,
        azul.plugins.repository.tdr_hca,
        azul.plugins.metadata.hca.indexer.transform,
        azul.service.drs_controller,
        azul.service.manifest_service,
        azul.service.repository_controller,
        azul.strings,
        azul.terra,
        azul.terraform,
        azul.threads,
        azul.time,
        azul.types,
        azul.uuids,
        azul.vendored.frozendict,
        load_app_module('service', unit_test=True),
        load_script('can_bundle'),
        load_script('envhook'),
        load_script('export_environment'),
        load_module(root + '/.flake8/azul_flake8.py', 'azul_flake8'),
        test_tagging,
        service
    ]:
        suite = doctest.DocTestSuite(module)
        assert suite.countTestCases() > 0, module
        tests.addTests(suite)
    return tests


if __name__ == '__main__':
    unittest.main()
