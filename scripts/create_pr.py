import argparse
import subprocess
import sys
from pathlib import (
    Path,
)

_project_root = Path(__file__).resolve().parent.parent
_template_dir = _project_root / '.github' / 'PULL_REQUEST_TEMPLATE'


def main(argv):
    templates = sorted(p.stem for p in _template_dir.glob('*.md'))
    parser = argparse.ArgumentParser(description='Create a pull request')
    parser.add_argument('--template', '-t',
                        default=None,
                        choices=templates,
                        help='Name of the PR template to use. '
                             'If omitted, the default template is used.')
    args = parser.parse_args(argv)

    if args.template is None:
        template_path = _project_root / '.github' / 'pull_request_template.md'
    else:
        template_path = _template_dir / f'{args.template}.md'

    cmd = [
        'gh', 'pr', 'create',
        '--title', 'FIXME',
        '--body-file', str(template_path),
    ]
    sys.exit(subprocess.run(cmd).returncode)


if __name__ == '__main__':
    main(sys.argv[1:])