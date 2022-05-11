import os
from pathlib import (
    Path,
)
import re
import shlex
import sys
from textwrap import (
    dedent,
)
from typing import (
    List,
    Match,
    NamedTuple,
    Optional,
    Tuple,
)

"""
Convert an old-style `environment` Bash script to a new-style `environment.py`.

This is by no means a complete parser for shell scripts. It recognizes only the
script statements typically used in `environment` and `environment.local` files.
"""

from azul.files import (
    write_file_atomically,
)


class Variable(NamedTuple):
    name: str
    value: str
    comments: List[str]


def convert(path: Path):
    output_path = convert_path(path)
    if path.is_symlink():
        output_link_path = convert_path(Path(os.readlink(str(path))))
        print(f'Linking {output_path} to {output_link_path}')
        if output_path.exists():
            output_path.unlink()
        output_path.symlink_to(output_link_path, )
    else:
        print(f'Reading {path}')
        variables, comments = read(path)
        print(f'Writing {output_path}')
        write(output_path, variables, comments)


def convert_path(path: Path):
    path = str(path)
    if path.endswith('_'):
        path = path[:-1]
    return Path(path + '.py')


def read(path: Path) -> Tuple[List[Variable], List[str]]:
    comments: List[str] = []
    variables: List[Variable] = []
    with open(str(path), 'r') as input_:
        for line in input_:
            try:
                if line == '\n':
                    comments.append(line)
                elif line.startswith('#'):
                    comments.append(line)
                elif line.startswith('_set'):
                    _set, name, value = shlex.split(line)
                    assert _set == '_set'
                    variables.append(
                        Variable(name=name, value=value, comments=comments))
                    comments = []
                elif line.startswith('export'):
                    export, assignment = shlex.split(line)
                    assert export == 'export'
                    name, _, value = assignment.partition('=')
                    variables.append(
                        Variable(name=name, value=value, comments=comments))
                    comments = []
                elif line.startswith('source'):
                    print('Warning: cannot convert line, commenting it out: ',
                          line.strip())
                    comments.append('# ' + line)
                else:
                    assert False
            except Exception:
                print(f'Error in line {line!r}')
                raise
    return variables, comments


def write(output_path: Path, variables: List[Variable], comments: List[str]):
    with write_file_atomically(output_path) as output:
        output.write(dedent('''
            from typing import Optional, Mapping


            def env() -> Mapping[str, Optional[str]]:
                """
                Returns a dictionary that maps environment variable names to values. The
                values are either None or strings. String values can contain references to
                other environment variables in the form `{FOO}` where FOO is the name of an
                environment variable. See

                https://docs.python.org/3.9/library/string.html#format-string-syntax

                for the concrete syntax. These references will be resolved *after* the
                overall environment has been compiled by merging all relevant
                `environment.py` and `environment.local.py` files.

                Entries with a `None` value will be excluded from the environment. They
                can be used to document a variable without a default value in which case
                other, more specific `environment.py` or `environment.local.py` files must
                provide the value.
                """
                return {
        '''[1:]))
        indent = '    '

        for variable in variables:
            for comment in variable.comments:
                output.write(indent * 2 + comment)
            output.write(f"{indent * 2}'{variable.name}': {convert_value(variable.value)},\n")
        for comment in comments:
            output.write(indent * 2 + comment)
        output.write(indent + '}\n')


def convert_value(value: str) -> Optional[str]:
    if value == "~null":
        return None
    else:
        # Convert shell-style interpolations to Python str.format() templates.
        # Translate `$foo` and `${foo}` to `{foo}`. Quote `{foo}` as `{{foo}}`.
        def sub(m: Match):
            if m.group().startswith('$'):
                variable_name = m[1] or m[2]
                return '{' + variable_name + '}'
            else:
                return '{{' + m[1] + '}}'

        value = re.sub(r'\$?{([^}]+)}|\$([_A-Za-z][_A-Za-z0-9]*)', sub, value)
        return f"'{value}'"


if __name__ == '__main__':
    for arg in sys.argv[1:]:
        convert(Path(arg))
