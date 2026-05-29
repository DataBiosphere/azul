"""
Create or update a PR for the current branch, takeing care of some of the CL
items in the template. Uses the default PR template unless the --type option is
passed. Use --help to see which types are currently supported.

The script infers the linked issue from the name of the currently checked out
branch, so make sure that the branch name matches our conventions. The inferral
is straight-forward for the default type, but it also supports the other types.

For the default type, the script guesses whether to prefix the PR title with
"Fix: " but since there is some ambiguity for debt issues you can override the
guess with the --fix or --no-fix options.
"""
import argparse
import json
import logging
from pathlib import (
    Path,
)
import re
import subprocess
import sys
from typing import (
    Literal,
)

import attrs
from furl import (
    furl,
)

from azul.lib import (
    R,
)
from azul.lib.strings import (
    format_and_dedent as fd,
)
from azul.lib.types import (
    check_type,
)
from azul.logging import (
    configure_script_logging,
)

log = logging.getLogger(__name__)

_project_root = Path(__file__).resolve().parent.parent
_template_dir = _project_root / '.github' / 'PULL_REQUEST_TEMPLATE'
_project_owner = 'DataBiosphere'
_project_title = 'Azul'


def main(argv):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--type', '-t',
                        default=None,
                        choices=['upgrade', 'promotion'],
                        help='Type of PR to create. '
                             'If omitted, a regular PR is created.')
    parser.add_argument('--no-partial',
                        action='store_true', default=False,
                        help='Remove partial label and check partiality tasks.')
    parser.add_argument('--no-reindex',
                        action='store_true', default=False,
                        help='Remove reindex labels and check reindex tasks.')
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
    if args.type is not None and args.no_partial:
        parser.error('--no-partial cannot be used with --type')
    if args.type is not None and args.no_reindex:
        parser.error('--no-reindex cannot be used with --type')

    branch = _current_branch()
    _check_working_copy()
    log.info('Checking remote branch …')
    _check_remote_branch(branch)
    title_suffix = ''
    if args.type is None:
        template_path = _project_root / '.github' / 'pull_request_template.md'
        issue_number = _issue_number(branch)
    elif args.type == 'upgrade':
        template_path = _template_dir / 'upgrade.md'
        date = _upgrade_date(branch)
        log.info('Searching for upgrade issue …')
        issue_number = _issue_number_by_title(
            f'Upgrade software dependencies {date}'
        )
    elif args.type == 'promotion':
        date, target = _promotion_date_and_target(branch)
        template_path = _template_dir / f'{target}-promotion.md'
        log.info('Searching for promotion issue …')
        issue_number = _issue_number_by_title(
            f'Promotion {date}'
        )
        title_suffix = f' {target}'
    else:
        assert False, R('Unsupported template', args.type)
    log.info('Fetching issue #%d …', issue_number)
    issue = _issue_info(issue_number)
    if args.fix is None:
        fix = issue.type == 'Defect'
    else:
        fix = args.fix
    title = _pr_title(issue_number, issue.title, fix, suffix=title_suffix)

    log.info('Checking for existing PR …')
    existing_pr = _existing_pr()
    template = template_path.read_text()

    if existing_pr is None:
        body = template
    else:
        body = existing_pr['body']
        expected_comment = template.split('-->', maxsplit=1)[0]
        assert body.startswith(expected_comment), R(
            'Existing PR was created with a different template')

    # Normalize line endings from GitHub API responses
    body = '\n'.join(body.splitlines())

    body = _reference_issue_in_body(body, issue_number)

    m = re.search(r'^- \[[ x]] Target branch is `(.+?)`$',
                  template, flags=re.MULTILINE)
    assert m is not None, R('Target branch task not found in template')
    target_branch = m.group(1)
    target_branch_task = r'Target branch is `' + re.escape(target_branch) + '`'
    if existing_pr is None:
        body = _check_task(body, target_branch_task)
    else:
        base = existing_pr['baseRefName']
        if base == target_branch:
            body = _check_task(body, target_branch_task)
        else:
            log.warning('Target branch is %r, expected %r', base, target_branch)
            body = _check_task(body, target_branch_task, checked=False)

    if args.no_partial:
        assert not _has_commit_tag(target_branch, 'p'), R(
            '--no-partial cannot be used when a commit is tagged `p`')
        body = _check_task(body, r'Added `p` tag to titles of partial commits.*')
        body = _check_task(body, r'This PR is labeled `partial`.*')
        body = _check_task(body, r'This PR partially resolves .*')

    has_u_tag = _has_commit_tag(target_branch, 'u')
    body = _check_task(body, r'Added `u` tag to commit title.*', checked=has_u_tag)
    body = _check_task(body, r'This PR is labeled `upgrade`.*', checked=has_u_tag)

    if args.no_reindex:
        assert not _has_commit_tag(target_branch, 'r'), R(
            '--no-reindex cannot be used when a commit is tagged `r`')
        body = _check_task(body, r'Added `r` tag to commit title.*')
        reindex_labels = _reindex_labels(body)
        for label in reindex_labels:
            body = _check_task(body, r'This PR is labeled `' + re.escape(label) + '`.*')

    body = _check_task(body, 'PR is assigned to the author')
    body = _check_task(body, r'Status of PR is \*In progress\*')
    body = _check_task(body, 'Name of PR branch matches .*')

    if args.type is None:
        handle = _branch_handle(branch)
        log.info('Verifying GitHub user …')
        assert handle == _github_user(), R(
            'Branch name does not match GitHub user', handle)

    body = _check_task(body, r'Status of linked issues? is \*In progress\*')
    body = _check_task(body, 'PR description links to linked issues?')

    if existing_pr is None:
        log.info('Creating PR …')
        result = subprocess.run(
            [
                'gh', 'pr', 'create',
                '--base', target_branch,
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
        log.info('Updating PR …')
        subprocess.run(
            [
                'gh', 'pr', 'edit', pr_url,
                '--title', title,
                '--body', body,
                '--add-assignee', '@me',
            ],
            capture_output=True, text=True, check=True
        )
        log.info('PR URL is %r', pr_url)

    _label(pr_url, 'upgrade', mode='add' if has_u_tag else 'remove')

    if args.no_partial:
        _label(pr_url, 'partial', mode='remove')

    if args.no_reindex:
        for label in reindex_labels:
            _label(pr_url, label, mode='remove')

    log.info('Setting PR status …')
    pr_node_id = _node_id(pr_url)
    _set_status(pr_node_id, 'In Progress')
    log.info('Setting issue status …')
    issue_node_id = _node_id(issue.url)
    _set_status(issue_node_id, 'In Progress')


def _current_branch() -> str:
    result = subprocess.run(
        ['git', 'rev-parse', '--abbrev-ref', 'HEAD'],
        capture_output=True, text=True, check=True
    )
    return result.stdout.strip()


def _check_working_copy() -> None:
    result = subprocess.run(
        ['git', 'status', '--porcelain'],
        capture_output=True, text=True, check=True
    )
    if result.stdout.strip():
        log.warning('Working copy has uncommitted changes')


def _check_remote_branch(branch: str) -> None:
    result = subprocess.run(
        ['git', 'ls-remote', '--heads', 'github', branch],
        capture_output=True, text=True, check=True
    )
    assert result.stdout.strip(), R(
        'Branch does not exist on the github remote', branch)

    remote_sha = result.stdout.split()[0]
    local_sha = subprocess.run(
        ['git', 'rev-parse', 'HEAD'],
        capture_output=True, text=True, check=True
    ).stdout.strip()

    if local_sha != remote_sha:
        subprocess.run(
            ['git', 'fetch', 'github', branch],
            capture_output=True, text=True, check=True
        )
        result = subprocess.run(
            ['git', 'merge-base', '--is-ancestor', remote_sha, local_sha],
            capture_output=True, text=True
        )
        if result.returncode == 0:
            log.warning('Remote branch is behind local. A push is needed')
        else:
            log.warning('Remote and local branch diverge. A force push is needed')


def _commit_title_tags(title: str) -> set[str]:
    m = re.match(r'^\[([^]]*)]', title)
    if m is None:
        return set()
    else:
        return set(m.group(1).split())


def _has_commit_tag(target_branch: str, tag: str) -> bool:
    result = subprocess.run(
        ['git', 'log', '--format=%s', f'{target_branch}..HEAD'],
        capture_output=True, text=True, check=True
    )
    return any(
        tag in _commit_title_tags(title)
        for title in result.stdout.splitlines()
    )


def _issue_number(branch: str) -> int:
    m = re.fullmatch(r'issues/[^/]+/(\d+)-.*', branch)
    assert m is not None, R('Cannot extract issue number from branch name', branch)
    return int(m.group(1))


def _upgrade_date(branch: str) -> str:
    m = re.fullmatch(r'upgrades/(\d{4}-\d{2}-\d{2})', branch)
    assert m is not None, R('Cannot extract date from branch name', branch)
    return m.group(1)


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


def _promotion_date_and_target(branch: str) -> tuple[str, str]:
    m = re.fullmatch(r'promotions/(\d{4}-\d{2}-\d{2})-(.*)', branch)
    assert m is not None, R('Cannot extract date and target from branch name', branch)
    return m.group(1), m.group(2)


@attrs.frozen
class _IssueInfo:
    title: str
    type: str
    url: str


def _issue_info(issue_number: int) -> _IssueInfo:
    result = subprocess.run(
        [
            'gh', 'api', 'graphql',
            '-f', 'query=' + fd('''
                {{
                    repository(owner: "{owner}", name: "azul") {{
                        issue(number: {number}) {{
                            title
                            url
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
    return _IssueInfo(
        title=issue['title'],
        type=issue_type['name'] if issue_type else '',
        url=issue['url']
    )


def _pr_title(issue_number: int,
              issue_title: str,
              fix: bool,
              suffix: str = ''
              ) -> str:
    prefix = 'Fix: ' if fix else ''
    return f'{prefix}{issue_title}{suffix} (#{issue_number})'


def _existing_pr() -> dict | None:
    result = subprocess.run(
        ['gh', 'pr', 'view', '--json', 'url,body,baseRefName'],
        capture_output=True, text=True
    )
    if result.returncode != 0:
        return None
    return json.loads(result.stdout)


def _reference_issue_in_body(body: str, issue_number: int) -> str:
    body, n = re.subn(r'^(Linked issues?: *)#\d{1,5}',
                      rf'\1#{issue_number}',
                      body, flags=re.MULTILINE)
    assert n > 0, R('Linked issues reference not found in body')
    assert n < 2, R('Multiple linked issues references found in body')
    return body


def _reindex_labels(body: str) -> list[str]:
    return re.findall(r'^- \[[ x]] This PR is labeled `(reindex:\w+)`',
                      body, flags=re.MULTILINE)


def _check_task(body: str, task: str, checked: bool = True) -> str:
    mark = 'x' if checked else ' '
    body, n = re.subn(r'^- \[[ x]] (' + task + ')$',
                      r'- [' + mark + r'] \1',
                      body, flags=re.MULTILINE)
    assert n > 0, R('Task item not found in template', task)
    assert n < 2, R('Multiple matching task items found', task)
    return body


def _branch_handle(branch: str) -> str:
    m = re.fullmatch(r'issues/([^/]+)/\d+-.*', branch)
    assert m is not None, R('Cannot extract handle from branch name', branch)
    return m.group(1)


def _github_user() -> str:
    result = subprocess.run(
        ['gh', 'api', 'user', '--jq', '.login'],
        capture_output=True, text=True, check=True
    )
    return result.stdout.strip()


def _gh_item_type(url: str) -> str:
    path_kind = furl(url).path.segments[2]
    result = {'pull': 'pr', 'issues': 'issue'}.get(path_kind)
    assert result is not None, R('Cannot determine issue or PR from URL', url)
    return result


def _node_id(url: str) -> str:
    item_type = _gh_item_type(url)
    result = subprocess.run(
        ['gh', item_type, 'view', url, '--json', 'id', '--jq', '.id'],
        capture_output=True, text=True, check=True
    )
    return result.stdout.strip()


type LabelMode = Literal['add', 'remove']


def _label(item_url: str, label: str, *, mode: LabelMode) -> None:
    assert check_type(LabelMode, mode)
    item_type = _gh_item_type(item_url)
    verb = {'add': 'Adding', 'remove': 'Removing'}[mode]
    preposition = {'add': 'to', 'remove': 'from'}[mode]
    log.info('%s label %r %s %r …', verb, label, preposition, item_url)
    subprocess.run(
        ['gh', item_type, 'edit', item_url, f'--{mode}-label', label],
        capture_output=True, text=True
    )


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


if __name__ == '__main__':
    configure_script_logging(log)
    main(sys.argv[1:])
