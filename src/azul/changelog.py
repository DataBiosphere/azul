from collections import (
    OrderedDict,
)
import os.path
import sys

from azul import (
    config,
)
from azul.files import (
    write_file_atomically,
)
from azul.types import (
    JSON,
)

module_name = 'azul_changes'
variable_name = 'changes'


def changes() -> list[JSON]:
    from importlib import (
        import_module,
    )
    changelog_module = import_module(module_name)
    return getattr(changelog_module, variable_name)


def compact_changes(limit=None) -> list[JSON]:
    def title_first(item):
        k, v = item
        try:
            return 0, ('title', 'issues', 'upgrade').index(k), k, v
        except ValueError:
            return 1, k, v

    return [OrderedDict(sorted(change.items(), key=title_first))
            for change in changes()[:limit]]


def write_changes(output_dir_path):
    """
    Write the change log as a Python literal to a module in the given directory. We're using Python syntax because it
    can be looked up and loaded very easily. See changes().
    """
    with write_file_atomically(os.path.join(output_dir_path, module_name + '.py')) as f:
        # Write each change as a single line. I tried pprint() but it reorders the keys in dictionaries and its line
        # wrapping algorithm is creating a non-uniform output.
        f.write(variable_name + ' = [\n')
        for change in changelog()[variable_name]:
            f.write('    ' + repr(change) + ',\n')
        f.write(']\n')


def changelog() -> JSON:
    # noinspection PyPackageRequirements
    import yaml
    with open(os.path.join(config.project_root, 'CHANGELOG.yaml')) as f:
        return yaml.safe_load(f)


if __name__ == '__main__':
    write_changes(sys.argv[1])
