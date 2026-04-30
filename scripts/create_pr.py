import argparse
import json
import re
import subprocess
import sys
from pathlib import (
    Path,
)

_project_root = Path(__file__).resolve().parent.parent
_template_dir = _project_root / '.github' / 'PULL_REQUEST_TEMPLATE'


def _current_branch() -> str:
    result = subprocess.run(
        ['git', 'rev-parse', '--abbrev-ref', 'HEAD'],
        capture_output=True, text=True, check=True
    )
    return result.stdout.strip()


def _issue_number(branch: str) -> int:
    m = re.fullmatch(r'issues/[^/]+/(\d+)-.*', branch)
    if m is None:
        raise RuntimeError(f'Cannot extract issue number from branch name: {branch!r}')
    return int(m.group(1))


def _issue_title(issue_number: int) -> str:
    result = subprocess.run(
        ['gh', 'issue', 'view', str(issue_number), '--json', 'title', '--jq', '.title'],
        capture_output=True, text=True, check=True
    )
    return result.stdout.strip()


def _pr_title(issue_number: int, issue_title: str) -> str:
    return f'{issue_title} (#{issue_number})'


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

    branch = _current_branch()
    issue_number = _issue_number(branch)
    title = _pr_title(issue_number, _issue_title(issue_number))

    cmd = [
        'gh', 'pr', 'create',
        '--title', title,
        '--body-file', str(template_path),
    ]
    sys.exit(subprocess.run(cmd).returncode)


if __name__ == '__main__':
    main(sys.argv[1:])