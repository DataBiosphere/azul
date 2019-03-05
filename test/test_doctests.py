import doctest
import unittest

import azul
import azul.json_freeze
from azul.modules import load_module
import azul.threads
import azul.time
import azul.transformer
import azul.vendored.frozendict


def load_tests(loader, tests, ignore):
    tests.addTests(doctest.DocTestSuite(azul))
    tests.addTests(doctest.DocTestSuite(azul.threads))
    tests.addTests(doctest.DocTestSuite(azul.time))
    tests.addTests(doctest.DocTestSuite(azul.transformer))
    tests.addTests(doctest.DocTestSuite(azul.json_freeze))
    tests.addTests(doctest.DocTestSuite(azul.vendored.frozendict))
    tests.addTests(doctest.DocTestSuite(load_module(azul.config.project_root + '/scripts/envhook.py', 'envhook')))
    return tests


if __name__ == '__main__':
    unittest.main()
