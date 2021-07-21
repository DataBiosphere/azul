import ast
from textwrap import (
    dedent,
)
import tokenize
import unittest

from azul import (
    config,
)
from azul.modules import (
    load_module,
)
from azul_test_case import (
    AzulUnitTestCase,
)

azul_flake8 = load_module(f'{config.project_root}/.flake8/azul_flake8.py', 'azul_flake8')
ImportErrors = azul_flake8.ImportErrors


class TestAzulFlake8(AzulUnitTestCase):

    def test_import_syntax(self):
        self.assertFlake8Error(ImportErrors.same_line, """
            import sys, os
        """)

        self.assertNoErrors("""
            import os
            import sys
        """)

        self.assertNoErrors("""
            import sys as sys_
        """)

    def test_single_from_import_syntax(self):
        self.assertFlake8Error(ImportErrors.not_wrapped, """
            from os import mkdir
        """)

        self.assertFlake8Error(ImportErrors.missing_breaks, """
            from os import (mkdir)
        """)

        self.assertFlake8Error(ImportErrors.no_trailing_comma, """
            from os import (
                mkdir
            )
        """)

        self.assertNoErrors("""
            from os import (
                mkdir,
            )
        """)

        self.assertNoErrors("""
            from os import (
                mkdir as mkdir_,
            )
        """)

    def test_multiple_from_import_syntax(self):
        self.assertFlake8Error(ImportErrors.not_joined, """
            from os import (
                mkdir,
            )
            from os import (
                rmdir,
            )
        """)

        self.assertFlake8Error(azul_flake8.ImportErrors.same_line, """
            from os import (
                mkdir, rmdir,
            )
        """)

        self.assertNoErrors("""
            from os import (
                mkdir,
                rmdir,
            )
        """)

    def test_ordering(self):
        # Modules are first sorted by category of origin.
        # Python runtime must precede external dependencies of the project.
        self.assertFlake8Error(ImportErrors.statement_not_ordered, """
            import more_itertools
            import itertools
        """)

        # External dependencies of the project must precede the project itself.
        self.assertFlake8Error(ImportErrors.statement_not_ordered, """
            import azul
            import more_itertools
        """)

        # Modules of the same category are then sorted by lexicographically by name.
        self.assertFlake8Error(ImportErrors.statement_not_ordered, """
            import sys
            import os
        """)

        # This includes from imports.
        self.assertFlake8Error(ImportErrors.statement_not_ordered, """
            from sys import (
                exit,
            )
            from os import (
                mkdir,
            )
        """)

        # Module imports must precede from imports of the same module.
        self.assertFlake8Error(ImportErrors.statement_not_ordered, """
            from os import (
                mkdir,
            )
            import os
        """)

        # Symbols within from imports must also be sorted.
        self.assertFlake8Error(ImportErrors.symbol_not_ordered, """
            from os import (
                rmdir,
                mkdir,
            )
        """)

        # Putting it all together:
        self.assertNoErrors("""
            import itertools
            import os
            from os import (
                mkdir,
                rmdir,
            )
            import sys
            from sys import (
                exit,
            )
            import more_itertools
            from more_itertools import (
                one,
            )
            import azul
            from azul import (
                config,
            )
        """)

    def test_comments(self):
        self.assertNoErrors("""
            import sys # script should not be thrown off by comments in the source code
        """)

        self.assertNoErrors("""
            #!/usr/bin/python3
            '''
            Not every source file has its imports at the very top.
            '''
            import os
        """)

    def test_local_imports(self):
        self.assertNoErrors("""
            # imports in different blocks should be checked against each other
            # for correct ordering.
            import azul.strings

            def foo():
                import azul.modules

                def foo_too():
                    import azul.json

            def bar():
                import azul.chalice
        """)

    def assertFlake8Error(self, expected_err: azul_flake8.ImportErrors, source: str):
        self.assertEqual([expected_err.value], [err.msg for err in self._collect_errors(source)])

    def assertNoErrors(self, source: str):
        self.assertEqual([], self._collect_errors(source))

    def _collect_errors(self, source: str):
        source = dedent(source)
        lines = iter(source.split('\n'))

        def readline():
            return next(lines) + '\n'

        tokens = tokenize.generate_tokens(readline)
        tree = ast.parse(source)
        return azul_flake8.AzulImports(tree, tokens, '')._run()


if __name__ == '__main__':
    unittest.main()
