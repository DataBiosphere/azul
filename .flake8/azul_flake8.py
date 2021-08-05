import ast
from collections import (
    defaultdict,
)
import enum
from enum import (
    Enum,
)
import importlib.util
import logging
import sys
import tokenize
from tokenize import (
    TokenInfo,
)
from typing import (
    Iterable,
    List,
    Optional,
    Tuple,
    Union,
)

import attr
from more_itertools import (
    one,
)

from azul import (
    config,
)

log = logging.getLogger(__name__)


@enum.unique
class ImportErrors(Enum):
    same_line = 'AZUL101 more than one symbol imported per line'
    not_joined = 'AZUL102 symbols from the same module imported in separate statements'

    statement_not_ordered = 'AZUL111 import statements are not correctly ordered'
    symbol_not_ordered = 'AZUL112 symbols in from import are not correctly ordered'

    not_wrapped = 'AZUL131 from import lacks parentheses'
    missing_breaks = 'AZUL132 missing newline between parentheses and imported symbols'
    no_trailing_comma = 'AZUL133 symbol in from import lacks trailing comma'

    unresolvable = 'AZUL141 cannot resolve import statement from project root'


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


@attr.s(auto_attribs=True, kw_only=True, frozen=True, order=True)
class ModuleOrderInfo:
    module_type: ModuleType
    module_name: str
    is_from_import: bool

    @classmethod
    def normalize_module(cls, node: EitherImport) -> Tuple[str, bool]:
        if isinstance(node, ast.Import):
            module_name = one(node.names).name
            is_from_import = False
        elif isinstance(node, ast.ImportFrom):
            module_name = node.module
            is_from_import = True
        else:
            assert False, type(node)
        return module_name, is_from_import

    @classmethod
    def from_ast(cls, node: EitherImport) -> 'ModuleOrderInfo':
        """
        >>> def from_stmt(src): return ModuleOrderInfo.from_ast(one(ast.parse(src).body))

        >>> from_stmt('import azul.indexer, azul.service')
        Traceback (most recent call last):
        ...
        ValueError: too many items in iterable (expected 1)

        >>> from_stmt('import does_not_exist')
        Traceback (most recent call last):
        ...
        ImportError

        >>> from_stmt('import azul.indexer')
        ModuleOrderInfo(module_type=<ModuleType.internal: 3>, module_name='azul.indexer', is_from_import=False)

        >>> from_stmt('from azul.indexer import BaseIndexer')
        ModuleOrderInfo(module_type=<ModuleType.internal: 3>, module_name='azul.indexer', is_from_import=True)

        >>> from_stmt('import itertools')
        ModuleOrderInfo(module_type=<ModuleType.python_runtime: 1>, module_name='itertools', is_from_import=False)

        >>> from_stmt('from more_itertools import one') #doctest: +NORMALIZE_WHITESPACE
        ModuleOrderInfo(module_type=<ModuleType.external_dependency: 2>, \
                        module_name='more_itertools', \
                        is_from_import=True)
        """
        module_name, is_from_import = cls.normalize_module(node)
        module_type = ModuleType.from_module_name(module_name)
        return cls(module_type=module_type,
                   module_name=module_name,
                   is_from_import=is_from_import)


@attr.s(auto_attribs=True, kw_only=True, frozen=True)
class OrderedImport:
    node: EitherImport
    order_info: ModuleOrderInfo

    @classmethod
    def from_ast(cls, node: EitherImport) -> 'OrderedImport':
        return cls(node=node, order_info=ModuleOrderInfo.from_ast(node))

    def is_correct_order(self, other: 'OrderedImport') -> bool:
        return (self.node.lineno <= other.node.lineno) == (self.order_info <= other.order_info)


@attr.s(auto_attribs=True, kw_only=True, frozen=True)
class ErrorInfo:
    line: int
    column: int
    msg: str

    @classmethod
    def from_ast(cls, node: EitherImport, err: ImportErrors):
        return cls(line=node.lineno,
                   column=node.col_offset,
                   msg=err.value)

    def to_flake8_tuple(self) -> tuple:
        # flake8 requires a fourth attribute but it's not used
        return *attr.astuple(self), None


class ImportVisitor(ast.NodeVisitor):
    expected_resolution_failures = {
        'pydevd',
        # FIXME: Remove hacky import of SupportsLessThan
        #        https://github.com/DataBiosphere/azul/issues/2783
        '_typeshed'
    }

    def __init__(self, file_name: str, file_tokens: Iterable[TokenInfo]):
        super().__init__()
        self.file_name = file_name
        self.line_tokens = defaultdict(list)
        for token_info in file_tokens:
            self.line_tokens[token_info.start[0]].append(token_info)
        for line_tokens in self.line_tokens.values():
            line_tokens.sort(key=lambda token_info: token_info.start[1])
        self.errors: List[ErrorInfo] = []
        self.visited_order_info: List[OrderedImport] = []

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
        try:
            ordered_import = OrderedImport.from_ast(node)
        except ValueError:
            # Some other formatting error prevents the correct order from being
            # determined
            pass
        except ImportError:
            module_name, _ = ModuleOrderInfo.normalize_module(node)
            if module_name not in self.expected_resolution_failures:
                self._error(node, ImportErrors.unresolvable)
        else:
            # The order in which NodeVisitor traverses the syntax tree is unspecified
            # so we can't be sure which nodes have already been visited.
            # To prevent a single out-of-order import from causing errors on every
            # other line, we only compare each import statement with one other statement.
            # To prevent the same line from being reported twice, the error is always
            # reported for the node we're currently visiting, regardless of whether it
            # comes first or second.
            pred = self._visited_predecessor(node)
            succ = self._visited_successor(node)
            if pred is not None and not self._is_correct_order(pred, ordered_import):
                self._error(node, ImportErrors.statement_not_ordered)
            elif succ is not None and not self._is_correct_order(ordered_import, succ):
                self._error(node, ImportErrors.statement_not_ordered)
            self.visited_order_info.append(ordered_import)

    def check_joined_import(self, node: ast.ImportFrom) -> None:
        for visited in self.visited_order_info:
            if (
                isinstance(visited.node, ast.ImportFrom)
                and visited.node.module == node.module
                and self._is_same_block(node, visited.node)
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
        self.errors.append(ErrorInfo.from_ast(node, err))

    def _is_correct_order(self,
                          import1: OrderedImport,
                          import2: OrderedImport
                          ) -> bool:
        return import1.is_correct_order(import2) or not self._is_same_block(import1.node, import2.node)

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

    def _visited_successor(self, node: EitherImport) -> Optional[OrderedImport]:
        """
        Scan the list of previously visited nodes for the node with the lowest
        line number that is greater than the provided node's line number.
        """
        return min(filter(lambda t: t.node.lineno > node.lineno, self.visited_order_info),
                   key=lambda t: t.node.lineno,
                   default=None)

    def _visited_predecessor(self, node: EitherImport) -> Optional[OrderedImport]:
        """
        Scan the list of previously visited nodes for the node with the highest
        line number that is less than the provided node's line number.
        """
        return max(filter(lambda t: t.node.lineno < node.lineno, self.visited_order_info),
                   key=lambda t: t.node.lineno,
                   default=None)

    def _filtered_tokens(self, linenno):
        return [tokeninfo
                for tokeninfo in self.line_tokens[linenno]  # 1-based indexing for source code lines
                if tokeninfo.type not in (tokenize.COMMENT, tokenize.NEWLINE, tokenize.NL, tokenize.ENDMARKER)]


class AzulImports:
    name = 'azul_imports'
    version = 1.0

    # The constructor signature is subject to the restrictions documented at
    # https://flake8.pycqa.org/en/3.8.2/plugin-development/plugin-parameters.html#indicating-desired-data
    def __init__(self, tree, file_tokens, filename):
        self.tree = tree
        self.tokens = file_tokens
        self.file_name = filename

    def _run(self) -> List[ErrorInfo]:
        visitor = ImportVisitor(self.file_name, self.tokens)
        visitor.visit(self.tree)
        return visitor.errors

    def run(self) -> List[tuple]:
        return [err.to_flake8_tuple() for err in self._run()]
