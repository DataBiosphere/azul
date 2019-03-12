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
    root = azul.config.project_root
    tests.addTests(doctest.DocTestSuite(load_module(root + '/scripts/envhook.py', 'envhook')))
    tests.addTests(doctest.DocTestSuite(load_module(root + '/scripts/check_branch.py', 'check_branch')))
    return tests


if __name__ == '__main__':
    unittest.main()
