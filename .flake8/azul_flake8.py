import ast
from collections import (
    defaultdict,
)
import enum
from enum import (
    Enum,
)
import importlib.util
import sys
import tokenize
from tokenize import (
    TokenInfo,
)
from typing import (
    Any,
    Iterable,
    List,
    NamedTuple,
    Optional,
    Tuple,
    Union,
)

from more_itertools import (
    one,
)

from azul import (
    config,
)


@enum.unique
class ImportErrors(Enum):
    same_line = 'AZUL101 more than one symbol imported per line'
    not_joined = ('AZUL102 symbols from the same module imported in separate '
                  'statements')

    statement_not_ordered = 'AZUL111 import statements are not correctly ordered'
    symbol_not_ordered = 'AZUL112 symbols in from import are not correctly ordered'

    not_wrapped = 'AZUL131 from import lacks parentheses'
    missing_breaks = ('AZUL132 missing newline between parentheses and imported '
                      'symbols')
    no_trailing_comma = 'AZUL133 symbol in from import lacks trailing comma'


@enum.unique
class ModuleType(enum.IntEnum):
    # Values determine sorting order and correspond to the ordering defined in
    # the contributing guide.
    python_runtime = 1
    external_dependency = 2
    internal = 3

    @classmethod
    def from_module_name(cls, module_name: str) -> 'ModuleType':
        """
        Infer module origin by inspecting the spec from its resolved name.

        A builtin module
        >>> ModuleType.from_module_name('sys')
        <ModuleType.python_runtime: 1>

        # A non-builtin standrad library module
        >>> ModuleType.from_module_name('os')
        <ModuleType.python_runtime: 1>

        # A downloaded external package
        >>> ModuleType.from_module_name('boto3')
        <ModuleType.external_dependency: 2>

        # A non-namespace internal module
        >>> ModuleType.from_module_name('azul.indexer')
        <ModuleType.internal: 3>

        # A non-namespace internal module outside the main src/ folder.
        # The flake8-import-order plugin struggles with these.
        >>> ModuleType.from_module_name('azul_test_case')
        <ModuleType.internal: 3>

        # A namespace internal module
        >>> ModuleType.from_module_name('lambdas.service')
        <ModuleType.internal: 3>
        """
        spec = importlib.util.find_spec(module_name)
        if spec is None:
            raise ImportError
        elif spec.origin is None and spec.submodule_search_locations is None:
            # Not every module included in the Python standard library is
            # considered "builtin." That term is reserved for modules such as
            # `sys` that are implemented in C and lack an associated .py file.
            assert module_name in sys.builtin_module_names, module_name
            return cls.python_runtime
        else:
            if spec.origin is None:
                # Namespace modules lack an __init__.py and represent their
                # location as a list of directories.
                path = one(spec.submodule_search_locations)
            else:
                path = spec.origin
            if 'site-packages' in path:
                # Assuming dependencies are installed in a virtual environment.
                # More specific path test would fail in travis.
                return cls.external_dependency
            elif path.startswith(config.project_root):
                return cls.internal
            else:
                # Assume any non-builtin module imported from outside the
                # project is part of the standard library. Python installation
                # locations can vary.
                return cls.python_runtime


EitherImport = Union[ast.Import, ast.ImportFrom]


class ModuleOrderInfo(NamedTuple):
    module_type: ModuleType
    module_name: str
    is_from_import: bool

    @classmethod
    def from_ast(cls, node: EitherImport) -> Optional['ModuleOrderInfo']:
        """
        >>> node = one(ast.parse('import azul.indexer, azul.service').body)
        >>> ModuleOrderInfo.from_ast(node)

        >>> node = one(ast.parse('import azul.indexer').body)
        >>> tuple(ModuleOrderInfo.from_ast(node))
        (<ModuleType.internal: 3>, 'azul.indexer', False)

        >>> node = one(ast.parse('from azul.indexer import BaseIndexer').body)
        >>> tuple(ModuleOrderInfo.from_ast(node))
        (<ModuleType.internal: 3>, 'azul.indexer', True)

        >>> node = one(ast.parse('import itertools').body)
        >>> tuple(ModuleOrderInfo.from_ast(node))
        (<ModuleType.python_runtime: 1>, 'itertools', False)

        >>> node = one(ast.parse('from more_itertools import one').body)
        >>> tuple(ModuleOrderInfo.from_ast(node))
        (<ModuleType.external_dependency: 2>, 'more_itertools', True)
        """
        if isinstance(node, ast.Import):
            try:
                module_name = one(node.names).name
            except ValueError:
                # Don't bother checking order on imports that aren't split
                return None
            else:
                is_from_import = False
        elif isinstance(node, ast.ImportFrom):
            module_name = node.module
            is_from_import = True
        else:
            assert False, type(node)
        try:
            module_type = ModuleType.from_module_name(module_name)
        except ImportError:
            # Failed to find module spec
            return None
        else:
            return ModuleOrderInfo(module_type, module_name, is_from_import)


class ErrorInfo(NamedTuple):
    line: int
    column: int
    msg: str
    unknown_field: Any  # flake8 requires a fourth element but it's not used


class ImportVisitor(ast.NodeVisitor):

    def __init__(self, file_tokens: Iterable[TokenInfo]):
        super().__init__()
        self.line_tokens = defaultdict(list)
        for token_info in file_tokens:
            self.line_tokens[token_info.start[0]].append(token_info)
        for line_tokens in self.line_tokens.values():
            line_tokens.sort(key=lambda token_info: token_info.start[1])
        self.errors: List[ErrorInfo] = []
        self.visited_order_info: List[Tuple[EitherImport, ModuleOrderInfo]] = []

    def visit_Import(self, node: ast.Import) -> None:
        self.check_split_import(node)
        self.check_statement_order(node)

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        self.check_joined_import(node)
        self.check_wrapping(node)
        self.check_symbol_order(node)
        self.check_statement_order(node)

    def check_split_import(self, node: EitherImport) -> None:
        if len(node.names) > 1:
            self._error(node, ImportErrors.same_line)

    def check_statement_order(self, node: EitherImport) -> None:
        order_info = ModuleOrderInfo.from_ast(node)
        if order_info is not None:
            # The order in which NodeVisitor traverses the syntax tree is unspecified
            # so we can't be sure which nodes have already been visited.
            # To prevent a single out-of-order import from causing errors on every
            # other line, we only compare each import statement with one other statement.
            # To prevent the same line from being reported twice, the error is always
            # reported for the node we're currently visiting, regardless of whether it
            # comes first or second.
            pred = self._visited_predecessor(node)
            succ = self._visited_successor(node)
            if (
                pred is not None
                and not self._is_correct_order(pred[0], node, pred[1], order_info)
            ):
                self._error(node, ImportErrors.statement_not_ordered)
            elif (
                succ is not None
                and not self._is_correct_order(node, succ[0], order_info, succ[1])
            ):
                self._error(node, ImportErrors.statement_not_ordered)
            self.visited_order_info.append((node, order_info))

    def check_joined_import(self, node: ast.ImportFrom) -> None:
        for visited, _ in self.visited_order_info:
            if (
                isinstance(visited, ast.ImportFrom)
                and visited.module == node.module
                and self._is_same_block(node, visited)
            ):
                self._error(node, ImportErrors.not_joined)
                break

    def check_wrapping(self, node: ast.ImportFrom) -> None:
        start_tokens = self._filtered_tokens(node.lineno)
        end_tokens = self._filtered_tokens(node.end_lineno)

        for i, t in enumerate(start_tokens):
            if t.string == '(':
                if i < len(start_tokens) - 1 or len(end_tokens) > 1:
                    self._error(node, ImportErrors.missing_breaks)
                break
        else:
            self._error(node, ImportErrors.not_wrapped)

        for i in range(node.lineno + 1, node.end_lineno):
            tokens = self._filtered_tokens(i)
            if not tokens:
                pass
            elif len(tokens) > 2 and (tokens[1].string != 'as' or len(tokens) > 4):
                self._error(node, ImportErrors.same_line)
            elif tokens[-1].string != ',':
                self._error(node, ImportErrors.no_trailing_comma)

    def check_symbol_order(self, node: ast.ImportFrom):
        for alias0, alias1 in zip(node.names, node.names[1:]):
            if alias0.name > alias1.name:
                self._error(node, ImportErrors.symbol_not_ordered)

    def _error(self, node: EitherImport, err: ImportErrors) -> None:
        self.errors.append(ErrorInfo(node.lineno, node.col_offset, err.value, None))

    def _is_correct_order(self,
                          node1: EitherImport,
                          node2: EitherImport,
                          order_info1: ModuleOrderInfo,
                          order_info2: ModuleOrderInfo):
        return (((node1.lineno <= node2.lineno) == (order_info1 <= order_info2))
                or not self._is_same_block(node1, node2))

    def _is_same_block(self, node1: EitherImport, node2: EitherImport):
        if node1.col_offset != node2.col_offset:
            return False
        else:
            if node1.lineno > node2.lineno:
                node1, node2 = node2, node1
            # Local imports in different functions, for example
            # Check for dedent
            return not any(line_tokens and line_tokens[0].start[1] < node1.col_offset
                           for line_tokens in map(self._filtered_tokens,
                                                  range(node1.lineno, node2.lineno)))

    def _visited_successor(self,
                           node: EitherImport
                           ) -> Optional[Tuple[EitherImport, ModuleOrderInfo]]:
        """
        Scan the list of previously visisted nodes for the node with the lowest
        line number that is greater than the provided node's line number.
        """
        return min(filter(lambda t: t[0].lineno > node.lineno, self.visited_order_info),
                   key=lambda t: t[0].lineno,
                   default=None)

    def _visited_predecessor(self,
                             node: EitherImport
                             ) -> Optional[Tuple[EitherImport, ModuleOrderInfo]]:
        """
        Scan the list of previously visisted nodes for the node with the highest
        line number that is less than the provided node's line number.
        """
        return max(filter(lambda t: t[0].lineno < node.lineno, self.visited_order_info),
                   key=lambda t: t[0].lineno,
                   default=None)

    def _filtered_tokens(self, linenno):
        return [
            tokeninfo
            # 1-based indexing for source code lines
            for tokeninfo in self.line_tokens[linenno]
            if tokeninfo.type not in (
                tokenize.COMMENT,
                tokenize.NEWLINE,
                tokenize.NL,
                tokenize.ENDMARKER
            )
        ]


class AzulImports:
    name = 'azul_imports'
    version = 1.0

    def __init__(self, tree, file_tokens):
        self.tree = tree
        self.tokens = file_tokens

    def run(self):
        visitor = ImportVisitor(self.tokens)
        visitor.visit(self.tree)
        return visitor.errors


class AzulLines:
    name = 'azul_lines'
    version = 1.0

    begin_skip = '# noqa lines: begin'
    end_skip = '# noqa lines: end'

    def __init__(self, tree, lines):
        # We need tree as a parameter otherwise run() won't be called
        self.lines = lines

    def run(self):
        errors: List[ErrorInfo] = []
        for line_num, line in self.unskipped_lines():
            error = self.check_length(line_num, line)
            if error is not None:
                errors.append(error)
        return errors

    def unskipped_lines(self):
        skipping = False
        for num, line in enumerate(self.lines):
            num += 1
            rstrip = line.rstrip()
            if rstrip.endswith(self.begin_skip):
                assert not skipping, (num, line)
                skipping = True
            elif rstrip.endswith(self.end_skip):
                assert skipping, (num, line)
                skipping = False
            if not skipping:
                yield num, line
        assert not skipping

    def check_length(self, line_num: int, physical_line) -> Optional[ErrorInfo]:
        line = physical_line.rstrip('\n')
        trimmed = line.lstrip(' ')
        # Exclude lines that end with URLs because we'd rather not break them into
        # multiple lines
        if len(line.split()) > 0 and not line.split()[-1].startswith('http'):
            if line.startswith('#') and len(line) > 80:
                return ErrorInfo(line=line_num,
                                 column=80,
                                 msg=f'AZUL201 comment length {len(line)} > 80',
                                 unknown_field=None)
            elif len(trimmed) > 80:
                return ErrorInfo(line=line_num,
                                 column=len(line) - len(trimmed) + 80,
                                 msg=f'AZUL202 trimmed line length {len(trimmed)} > 80',
                                 unknown_field=None)


def flake8_plugin(**attrs):
    def decorator(f):
        f.name = f.__name__
        for k, v in attrs.items():
            setattr(f, k, v)
        return f

    return decorator


@flake8_plugin(version=1.0)
def example_scan(physical_line):
    column = 1
    if column != 1:
        return column, 'AZUL999 this is an example'
