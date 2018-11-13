import doctest
import unittest

import azul.transformer
import azul.json_freeze
import azul.vendored.frozendict


def load_tests(loader, tests, ignore):
    tests.addTests(doctest.DocTestSuite(azul))
    tests.addTests(doctest.DocTestSuite(azul.transformer))
    tests.addTests(doctest.DocTestSuite(azul.json_freeze))
    tests.addTests(doctest.DocTestSuite(azul.vendored.frozendict))
    return tests


if __name__ == '__main__':
    unittest.main()
