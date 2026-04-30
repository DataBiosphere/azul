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
_project_title = 'Azul'


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


def _upgrade_date(branch: str) -> str:
    m = re.fullmatch(r'upgrades/(\d{4}-\d{2}-\d{2})', branch)
    assert m is not None, R('Cannot extract date from branch name', branch)
    return m.group(1)


def _promotion_date_and_target(branch: str) -> tuple[str, str]:
    m = re.fullmatch(r'promotions/(\d{4}-\d{2}-\d{2})-(.*)', branch)
    assert m is not None, R('Cannot extract date and target from branch name', branch)
    return m.group(1), m.group(2)


def _issue_number_by_title(title: str) -> int:
    result = subprocess.run(
        [
            'gh', 'issue', 'list',
            '--search', f'{title} in:title',
            '--state', 'all',
            '--json', 'number,title',
        ],
        capture_output=True, text=True, check=True
    )
    issues = json.loads(result.stdout)
    for issue in issues:
        if issue['title'] == title:
            return issue['number']
    assert False, R('Issue not found with title', title)


def _issue_info(issue_number: int) -> tuple[str, str]:
    result = subprocess.run(
        [
            'gh', 'api', 'graphql',
            '-f', 'query=' + fd('''
                {{
                    repository(owner: "{owner}", name: "azul") {{
                        issue(number: {number}) {{
                            title
                            issueType {{ name }}
                        }}
                    }}
                }}
            ''', owner=_project_owner, number=issue_number),
        ],
        capture_output=True, text=True, check=True
    )
    issue = json.loads(result.stdout)['data']['repository']['issue']
    issue_type = issue['issueType']
    return issue['title'], issue_type['name'] if issue_type else ''


def _pr_title(issue_number: int,
              issue_title: str,
              fix: bool,
              suffix: str = ''
              ) -> str:
    prefix = 'Fix: ' if fix else ''
    return f'{prefix}{issue_title}{suffix} (#{issue_number})'


def _check_task(body: str, task: str) -> str:
    body_new, n = re.subn(r'^- \[ ] (' + task + ')$',
                          r'- [x] \1',
                          body, count=1, flags=re.MULTILINE)
    if n > 0:
        return body_new
    assert re.search(r'^- \[x] ' + task + '$', body, flags=re.MULTILINE), R(
        'Task item not found in template', task)
    return body


def main(argv):
    parser = argparse.ArgumentParser(description='Create a pull request')
    parser.add_argument('--type', '-t',
                        default=None,
                        choices=['upgrade', 'promotion'],
                        help='Type of PR to create. '
                             'If omitted, a regular PR is created.')
    fix_group = parser.add_mutually_exclusive_group()
    fix_group.add_argument('--fix',
                           action='store_true', default=None,
                           help='Prefix the PR title with "Fix: ".')
    fix_group.add_argument('--no-fix',
                           action='store_false', dest='fix',
                           help='Do not prefix the PR title with "Fix: ".')
    args = parser.parse_args(argv)
    if args.type is not None and args.fix is not None:
        parser.error('--fix/--no-fix cannot be used with --type')

    branch = _current_branch()
    title_suffix = ''
    if args.type is None:
        template_path = _project_root / '.github' / 'pull_request_template.md'
        issue_number = _issue_number(branch)
    elif args.type == 'upgrade':
        template_path = _template_dir / 'upgrade.md'
        date = _upgrade_date(branch)
        issue_number = _issue_number_by_title(
            f'Upgrade software dependencies {date}'
        )
    elif args.type == 'promotion':
        date, target = _promotion_date_and_target(branch)
        template_path = _template_dir / f'{target}-promotion.md'
        issue_number = _issue_number_by_title(
            f'Promotion {date}'
        )
        title_suffix = f' {target}'
    else:
        assert False, R('Unsupported template', args.type)
    issue_title, issue_type = _issue_info(issue_number)
    if args.fix is None:
        fix = issue_type == 'Defect'
    else:
        fix = args.fix
    title = _pr_title(issue_number, issue_title, fix, suffix=title_suffix)

    existing_pr = _existing_pr()

    if existing_pr is None:
        body = template_path.read_text()
    else:
        body = existing_pr['body']
    # Normalize line endings from GitHub API responses
    body = '\n'.join(body.splitlines())
    body, n = re.subn(r'^(Linked issues?: *)#\d{1,5}',
                      rf'\1#{issue_number}',
                      body, flags=re.MULTILINE)
    assert n > 0, R('Linked issues reference not found in body')

    body = _check_task(body, 'PR is assigned to the author')
    body = _check_task(body, r'Status of PR is \*In progress\*')
    body = _check_task(body, r'Status of linked issues? is \*In progress\*')
    body = _check_task(body, 'PR description links to linked issues?')

    if existing_pr is None:
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
    else:
        pr_url = existing_pr['url']
        subprocess.run(
            [
                'gh', 'pr', 'edit', pr_url,
                '--title', title,
                '--body', body,
                '--add-assignee', '@me',
            ],
            capture_output=True, text=True, check=True
        )
        print(pr_url)

    pr_node_id = _node_id('pr', pr_url)
    issue_node_id = _node_id('issue', str(issue_number))
    _set_status(pr_node_id, 'In Progress')
    _set_status(issue_node_id, 'In Progress')


def _existing_pr() -> dict | None:
    result = subprocess.run(
        ['gh', 'pr', 'view', '--json', 'url,body'],
        capture_output=True, text=True
    )
    if result.returncode != 0:
        return None
    return json.loads(result.stdout)


def _node_id(kind: str, ref: str) -> str:
    result = subprocess.run(
        ['gh', kind, 'view', ref, '--json', 'id', '--jq', '.id'],
        capture_output=True, text=True, check=True
    )
    return result.stdout.strip()


def _set_status(node_id: str, status: str) -> None:
    project_id = _project_id()

    query = fd('''
        mutation {{
            addProjectV2ItemById(input: {{
                projectId: "{project_id}",
                contentId: "{node_id}"
            }}) {{ item {{ id }} }}
        }}
    ''', project_id=project_id, node_id=node_id)
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


def _project() -> dict:
    result = subprocess.run(
        [
            'gh', 'project', 'list',
            '--owner', _project_owner,
            '--format', 'json',
        ],
        capture_output=True, text=True, check=True
    )
    projects = json.loads(result.stdout)['projects']
    for project in projects:
        if project['title'] == _project_title:
            return project
    assert False, R('Project not found', _project_title)


def _project_id() -> str:
    query = fd('''
        {{
            organization(login: "{owner}") {{
                projectV2(number: {number}) {{ id }}
            }}
        }}
    ''', owner=_project_owner, number=_project()['number'])
    result = subprocess.run(
        ['gh', 'api', 'graphql', '-f', f'query={query}'],
        capture_output=True, text=True, check=True
    )
    return json.loads(result.stdout)['data']['organization']['projectV2']['id']


def _status_field() -> dict:
    result = subprocess.run(
        [
            'gh', 'project', 'field-list', str(_project()['number']),
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
