import argparse
import json
from pathlib import (
    Path,
)
import re
import subprocess
import sys

from azul.lib import (
    R,
)
from azul.lib.strings import (
    format_and_dedent as fd,
)

_project_root = Path(__file__).resolve().parent.parent
_template_dir = _project_root / '.github' / 'PULL_REQUEST_TEMPLATE'
_project_owner = 'DataBiosphere'
_project_number = 3


def _current_branch() -> str:
    result = subprocess.run(
        ['git', 'rev-parse', '--abbrev-ref', 'HEAD'],
        capture_output=True, text=True, check=True
    )
    return result.stdout.strip()


def _issue_number(branch: str) -> int:
    m = re.fullmatch(r'issues/[^/]+/(\d+)-.*', branch)
    assert m is not None, R('Cannot extract issue number from branch name', branch)
    return int(m.group(1))


def _issue_title(issue_number: int) -> str:
    result = subprocess.run(
        ['gh', 'issue', 'view', str(issue_number), '--json', 'title', '--jq', '.title'],
        capture_output=True, text=True, check=True
    )
    return result.stdout.strip()


def _pr_title(issue_number: int, issue_title: str) -> str:
    return f'{issue_title} (#{issue_number})'


def _check_task(body: str, task: str) -> str:
    old = f'- [ ] {task}'
    new = f'- [x] {task}'
    assert old in body, R('Task item not found in template', task)
    return body.replace(old, new, 1)


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

    body = template_path.read_text()
    body = body.replace('#0000', f'#{issue_number}', 1)
    body = _check_task(body, 'PR is assigned to the author')
    body = _check_task(body, 'Status of PR is *In progress*')
    body = _check_task(body, 'PR is linked to all issues it (partially) resolves')
    body = _check_task(body, 'PR description links to linked issues')

    result = subprocess.run(
        [
            'gh', 'pr', 'create',
            '--title', title,
            '--body', body,
            '--assignee', '@me',
        ],
        capture_output=True, text=True
    )
    sys.stdout.write(result.stdout)
    sys.stderr.write(result.stderr)
    if result.returncode != 0:
        sys.exit(result.returncode)

    pr_url = result.stdout.strip()
    _set_pr_status(pr_url, 'In Progress')


def _set_pr_status(pr_url: str, status: str) -> None:
    result = subprocess.run(
        [
            'gh', 'pr', 'view', pr_url,
            '--json', 'id',
            '--jq', '.id',
        ],
        capture_output=True, text=True, check=True
    )
    pr_node_id = result.stdout.strip()

    project_id = _project_id()

    query = fd('''
        mutation {{
            addProjectV2ItemById(input: {{
                projectId: "{project_id}",
                contentId: "{pr_node_id}"
            }}) {{ item {{ id }} }}
        }}
    ''', project_id=project_id, pr_node_id=pr_node_id)
    result = subprocess.run(
        ['gh', 'api', 'graphql', '-f', f'query={query}'],
        capture_output=True, text=True, check=True
    )
    item_id = json.loads(result.stdout)['data']['addProjectV2ItemById']['item']['id']

    status_field = _status_field()
    option_id = _status_option_id(status_field, status)

    query = fd('''
        mutation {{
            updateProjectV2ItemFieldValue(input: {{
                projectId: "{project_id}",
                itemId: "{item_id}",
                fieldId: "{field_id}",
                value: {{singleSelectOptionId: "{option_id}"}}
            }}) {{ projectV2Item {{ id }} }}
        }}
    ''', project_id=project_id, item_id=item_id,
               field_id=status_field['id'], option_id=option_id)
    subprocess.run(
        ['gh', 'api', 'graphql', '-f', f'query={query}'],
        capture_output=True, text=True, check=True
    )


def _project_id() -> str:
    query = fd('''
        {{
            organization(login: "{owner}") {{
                projectV2(number: {number}) {{ id }}
            }}
        }}
    ''', owner=_project_owner, number=_project_number)
    result = subprocess.run(
        ['gh', 'api', 'graphql', '-f', f'query={query}'],
        capture_output=True, text=True, check=True
    )
    return json.loads(result.stdout)['data']['organization']['projectV2']['id']


def _status_field() -> dict:
    result = subprocess.run(
        [
            'gh', 'project', 'field-list', str(_project_number),
            '--owner', _project_owner,
            '--format', 'json',
        ],
        capture_output=True, text=True, check=True
    )
    fields = json.loads(result.stdout)['fields']
    for field in fields:
        if field['name'] == 'Status':
            return field
    assert False, R('Status field not found in project')


def _status_option_id(status_field: dict, status: str) -> str:
    for option in status_field['options']:
        if option['name'] == status:
            return option['id']
    assert False, R('Status option not found', status)


if __name__ == '__main__':
    main(sys.argv[1:])
