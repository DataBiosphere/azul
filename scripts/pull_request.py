"""
Create or update a PR for the current branch, takeing care of some of the CL
items in the template. Uses the default PR template unless the --type option is
passed. Use --help to see which types are currently supported.

Unless the --issues option names the linked issues, the script infers them from
the name of the currently checked out branch, so make sure that the branch name
matches our conventions. A branch that resolves more than one issue names them
all. The inferral is straight-forward for the default type, but it also supports
the other types. Either way, the PR refers to, links and advances every linked
issue, while its title is derived from the first one.

For the default type, the script guesses whether to prefix the PR title with
"Fix: " but since there is some ambiguity for debt issues you can override the
guess with the --fix or --no-fix options.
"""
import argparse
from argparse import (
    Namespace,
)
import json
import logging
from pathlib import (
    Path,
)
import re
import subprocess
import sys

import attrs
from furl import (
    furl,
)

from azul.lib import (
    R,
    cached_property,
)
from azul.lib.strings import (
    format_and_dedent as fd,
    join_grammatically,
)
from azul.lib.types import (
    JSON,
    json_str,
)
from azul.logging import (
    configure_script_logging,
)

log = logging.getLogger(__name__)

_project_root = Path(__file__).resolve().parent.parent
_template_dir = _project_root / '.github' / 'PULL_REQUEST_TEMPLATE'
_project_owner = 'DataBiosphere'
_project_repos = ['azul', 'azul-private']
_project_title = 'Azul'

#: Matches the label that prefixes every checklist item in the PR templates
#:
_item_label = r'`[A-Z]\d\d` '


@attrs.define(slots=False)
class Main:
    argv: list[str]
    _body: str | None = attrs.field(init=False, default=None)
    labels_to_add: list[str] = attrs.field(init=False, factory=list)
    labels_to_remove: list[str] = attrs.field(init=False, factory=list)

    def __attrs_post_init__(self):
        self._check_working_copy()
        self._check_remote_branch()
        self._verify_user()

    @property
    def body(self) -> str:
        if self._body is None:
            if self._existing_pr is None:
                self._body = self._template
            else:
                self._body = json_str(self._existing_pr['body'])
                expected_comment = self._template.split('-->', maxsplit=1)[0]
                assert self._body.startswith(expected_comment), R(
                    'Existing PR was created with a different template')
            # Normalize line endings from GitHub API responses
            self._body = '\n'.join(self._body.splitlines())
        return self._body

    @body.setter
    def body(self, body: str) -> None:
        self._body = body

    @cached_property
    def _args(self) -> Namespace:
        parser = argparse.ArgumentParser(description=__doc__)
        parser.add_argument('--type', '-t',
                            default=None,
                            choices=['upgrade', 'promotion'],
                            help='Type of PR to create. '
                                 'If omitted, a regular PR is created.')
        parser.add_argument('--issues', '-i',
                            nargs='+', type=int, default=None, metavar='NUMBER',
                            help='The numbers of the issues linked to this PR, the '
                                 'first of which determines the PR title. '
                                 'If omitted, the issues are inferred from the name '
                                 'of the current branch.')
        section_flags = {
            'partial': 'Remove partial label and check partiality tasks.',
            'mirror': 'Remove mirror labels and check mirror tasks.',
            'reindex': 'Remove reindex labels and check reindex tasks.',
            'api': 'Remove API label and check API tasks.',
            'upgrade': 'Remove upgrade label and check upgrade tasks.',
            'deploy': 'Remove deploy labels and check deploy tasks.',
            'hotfix': 'Check hotfix tasks.',
            'reqs': 'Remove reqs label and check requirements tasks.',
        }
        for flag, help_text in section_flags.items():
            parser.add_argument(f'--no-{flag}',
                                action='store_true', default=False,
                                help=help_text)
        all_no_flags = join_grammatically([f'--no-{f}' for f in section_flags])
        parser.add_argument('--vanilla',
                            action='store_true', default=False,
                            help=f'Equivalent to {all_no_flags}.')
        fix_group = parser.add_mutually_exclusive_group()
        fix_group.add_argument('--fix',
                               action='store_true', default=None,
                               help='Prefix the PR title with "Fix: ".')
        fix_group.add_argument('--no-fix',
                               action='store_false', dest='fix',
                               help='Do not prefix the PR title with "Fix: ".')
        args = parser.parse_args(self.argv)
        if args.vanilla:
            for flag in section_flags:
                setattr(args, f'no_{flag}', True)
        if args.type is not None and args.fix is not None:
            parser.error('--fix/--no-fix cannot be used with --type')
        if args.type is not None:
            for flag in section_flags:
                if getattr(args, f'no_{flag}'):
                    parser.error(f'--no-{flag} cannot be used with --type')
        return args

    def main(self):
        self._add_issue_references_to_body()
        self._handle_target_branch()
        self._handle_partiality()
        self._handle_upgrades()
        self._handle_deploy()
        self._handle_reindex()
        self._handle_mirror()
        self._handle_api()
        self._handle_hotfix()
        self._handle_reqs()

        self._check_task('PR is assigned to the author')
        self._check_task(r'Status of PR is \*In progress\*')
        self._check_task('Name of PR branch matches .*')

        self._check_task('PR is linked to .*')
        self._check_task(r'Status of linked issues? is \*In progress\*')
        self._check_task('PR description links to linked issues?')

        if self._existing_pr is None:
            pr_url = self._create_pr()
        else:
            pr_url = json_str(self._existing_pr['url'])
            self._update_pr(pr_url)

        log.info('Setting PR status …')
        pr_node_id = self._node_id(pr_url)
        self._set_status(pr_node_id, 'In Progress')

        for issue in self._issues:
            issue_node_id = self._node_id(issue.url)
            log.info('Linking PR to issue %s …', issue.ref)
            self._link_issue(issue_node_id, pr_node_id)
            log.info('Setting status of issue %s …', issue.ref)
            self._set_status(issue_node_id, 'In Progress')

    def _update_pr(self, pr_url: str) -> None:
        log.info('Updating PR …')
        cmd = [
            'gh', 'pr', 'edit', pr_url,
            '--title', self._pr_title,
            '--body', self.body,
            '--add-assignee', '@me',
        ]
        if self.labels_to_add:
            cmd.extend(['--add-label', ','.join(self.labels_to_add)])
        if self.labels_to_remove:
            cmd.extend(['--remove-label', ','.join(self.labels_to_remove)])
        subprocess.run(cmd, capture_output=True, text=True, check=True)
        log.info('PR URL is %r', pr_url)

    def _create_pr(self) -> str:
        log.info('Creating PR …')
        cmd = [
            'gh', 'pr', 'create',
            '--base', self._target_branch,
            '--title', self._pr_title,
            '--body', self.body,
            '--assignee', '@me',
        ]
        if self.labels_to_add:
            cmd.extend(['--label', ','.join(self.labels_to_add)])
        result = subprocess.run(cmd, capture_output=True, text=True)
        # The output includes the PR URL, so we don't need to log it separately
        sys.stdout.write(result.stdout)
        sys.stderr.write(result.stderr)
        if result.returncode != 0:
            sys.exit(result.returncode)
        pr_url = result.stdout.strip()
        return pr_url

    def _verify_user(self):
        if self._args.type is None:
            handle = self._branch_handle
            log.info('Verifying GitHub user …')
            assert handle == self._github_user(), R(
                'Branch name does not match GitHub user', handle)

    def _handle_reqs(self):
        if self._args.no_reqs:
            assert not self._has_commit_tag('R'), R(
                '--no-reqs cannot be used when a commit is tagged `R`')
            self._check_task(r'Ran `make requirements_update`.*')
            self._check_task(r'Added `R` tag to commit title.*')
            self._check_task(r'This PR is labeled `reqs`.*')
            self.labels_to_remove.append('reqs')

    def _handle_hotfix(self):
        if self._args.no_hotfix:
            assert not self._has_commit_tag('F'), R(
                '--no-hotfix cannot be used when a commit is tagged `F`')
            self._check_task(r'Added `F` tag to main commit title.*')
            self._check_task(r'Reverted the temporary hotfixes for any linked issues.*')

    def _handle_api(self):
        if self._args.no_api:
            assert not self._has_commit_tag('a'), R(
                '--no-api cannot be used when a commit is tagged `a`')
            assert not self._has_commit_tag('A'), R(
                '--no-api cannot be used when a commit is tagged `A`')
            self._check_task(r'This PR and its linked issues are labeled `API`.*')
            self._check_task(r'Added `a` \(`A`\) tag to commit title.*')
            self._check_task(r'Updated REST API version number in `app\.py`.*')
            self.labels_to_remove.append('API')

    def _handle_mirror(self):
        if self._args.no_mirror:
            mirror_labels = self._mirror_labels()
            for label in mirror_labels:
                self._check_task(r'This PR is labeled `' + re.escape(label) + '`.*')
            self.labels_to_remove.extend(mirror_labels)

    def _handle_reindex(self):
        if self._args.no_reindex:
            assert not self._has_commit_tag('r'), R(
                '--no-reindex cannot be used when a commit is tagged `r`')
            self._check_task(r'Added `r` tag to commit title.*')
            reindex_labels = self._reindex_labels()
            for label in reindex_labels:
                self._check_task(r'This PR is labeled `' + re.escape(label) + '`.*')
            self.labels_to_remove.extend(reindex_labels)

    def _handle_deploy(self):
        if self._args.no_deploy:
            deploy_labels = self._deploy_labels()
            for label in deploy_labels:
                self._check_task(r'This PR is labeled `' + re.escape(label) + '`.*')
            self.labels_to_remove.extend(deploy_labels)

    def _handle_upgrades(self):
        has_u_tag = self._has_commit_tag('u')
        assert not (has_u_tag and self._args.no_upgrade), R(
            '--no-upgrade cannot be used when a commit is tagged `u`')
        checked = has_u_tag or self._args.no_upgrade
        if self._args.type != 'promotion':
            self._check_task(r'Added `u` tag to commit title.*', checked=checked)
        self._check_task(r'This PR is labeled `upgrade`.*', checked=checked)
        if has_u_tag:
            self.labels_to_add.append('upgrade')
        else:
            self.labels_to_remove.append('upgrade')
        if self._args.no_upgrade:
            self._check_task(r'Ran `make docker_images\.json`.*')
            self._check_task(r'Documented upgrading of deployments in UPGRADING\.rst.*')

    def _handle_partiality(self):
        if self._args.no_partial:
            assert not self._has_commit_tag('p'), R(
                '--no-partial cannot be used when a commit is tagged `p`')
            self._check_task(r'Added `p` tag to titles of partial commits.*')
            self._check_task(r'This PR is labeled `partial`.*')
            self._check_task(r'This PR partially resolves .*')
            self.labels_to_remove.append('partial')

    def _handle_target_branch(self):
        task = r'Target branch is `' + re.escape(self._target_branch) + '`'
        if self._existing_pr is None:
            self._check_task(task)
        else:
            base = self._existing_pr['baseRefName']
            if base == self._target_branch:
                self._check_task(task)
            else:
                log.warning('Target branch is %r, expected %r', base, self._target_branch)
                self._check_task(task, checked=False)

    @cached_property
    def _template(self) -> str:
        return self._template_path.read_text()

    @cached_property
    def _target_branch(self) -> str:
        m = re.search(r'^- \[[ x]] ' + _item_label + r'Target branch is `(.+?)`$',
                      self._template, flags=re.MULTILINE)
        assert m is not None, R('Target branch task not found in template')
        return m.group(1)

    @cached_property
    def _issues(self) -> list[_IssueInfo]:
        issue_numbers = self._issue_numbers
        log.info('Fetching issue %s …',
                 join_grammatically([f'#{n}' for n in issue_numbers]))
        return [self._issue_info(issue_number) for issue_number in issue_numbers]

    @cached_property
    def _issue_refs(self) -> list[str]:
        return [issue.ref for issue in self._issues]

    @cached_property
    def _fix(self) -> bool:
        if self._args.fix is None:
            return self._issue.type == 'Defect'
        else:
            return self._args.fix

    @cached_property
    def _issue(self) -> _IssueInfo:
        # The PR is assumed to resolve every one of the linked issues. The first
        # of them determines the PR title.
        return self._issues[0]

    @cached_property
    def _issue_numbers(self) -> list[int]:
        issue_numbers = self._args.issues
        if issue_numbers is None:
            if self._args.type is None:
                issue_numbers = self._issue_numbers_from_branch_name
            elif self._args.type == 'upgrade':
                date = self._upgrade_date
                log.info('Searching for upgrade issue …')
                issue_numbers = [self._issue_number_by_title(
                    f'Upgrade software dependencies {date}'
                )]
            else:
                date, target = self._promotion_date_and_target
                log.info('Searching for promotion issue …')
                issue_numbers = [self._issue_number_by_title(
                    f'Promotion {date}'
                )]
        return issue_numbers

    @cached_property
    def _title_suffix(self) -> str:
        if self._args.type == 'promotion':
            date, target = self._promotion_date_and_target
            title_suffix = f' {target}'
        else:
            title_suffix = ''
        return title_suffix

    @cached_property
    def _template_path(self) -> Path:
        if self._args.type is None:
            template_path = _project_root / '.github' / 'pull_request_template.md'
        elif self._args.type == 'upgrade':
            template_path = _template_dir / 'upgrade.md'
        elif self._args.type == 'promotion':
            date, target = self._promotion_date_and_target
            template_path = _template_dir / f'{target}-promotion.md'
        else:
            assert False, R('Unsupported template', self._args.type)
        return template_path

    @cached_property
    def _branch(self) -> str:
        result = subprocess.run(
            ['git', 'rev-parse', '--abbrev-ref', 'HEAD'],
            capture_output=True, text=True, check=True
        )
        return result.stdout.strip()

    def _check_working_copy(self) -> None:
        result = subprocess.run(
            ['git', 'status', '--porcelain'],
            capture_output=True, text=True, check=True
        )
        if result.stdout.strip():
            log.warning('Working copy has uncommitted changes')

    def _check_remote_branch(self) -> None:
        log.info('Checking remote branch …')
        result = subprocess.run(
            ['git', 'ls-remote', '--heads', 'github', self._branch],
            capture_output=True, text=True, check=True
        )
        assert result.stdout.strip(), R(
            'Branch does not exist on the github remote', self._branch)

        remote_sha = result.stdout.split()[0]
        local_sha = subprocess.run(
            ['git', 'rev-parse', 'HEAD'],
            capture_output=True, text=True, check=True
        ).stdout.strip()

        if local_sha != remote_sha:
            subprocess.run(
                ['git', 'fetch', 'github', self._branch],
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

    def _commit_title_tags(self, title: str) -> set[str]:
        m = re.match(r'^\[([^]]*)]', title)
        if m is None:
            return set()
        else:
            return set(m.group(1).split())

    def _has_commit_tag(self, tag: str) -> bool:
        result = subprocess.run(
            ['git', 'log', '--format=%s', f'{self._target_branch}..HEAD'],
            capture_output=True, text=True, check=True
        )
        return any(
            tag in self._commit_title_tags(title)
            for title in result.stdout.splitlines()
        )

    @cached_property
    def _issue_numbers_from_branch_name(self) -> list[int]:
        m = re.fullmatch(r'issues/[^/]+/(\d+(?:-\d+)*)-.*', self._branch)
        assert m is not None, R(
            'Cannot extract issue numbers from branch name', self._branch)
        return [int(number) for number in m.group(1).split('-')]

    @cached_property
    def _upgrade_date(self) -> str:
        m = re.fullmatch(r'upgrades/(\d{4}-\d{2}-\d{2})', self._branch)
        assert m is not None, R('Cannot extract date from branch name', self._branch)
        return m.group(1)

    def _issue_number_by_title(self, title: str) -> int:
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

    @cached_property
    def _promotion_date_and_target(self) -> tuple[str, str]:
        m = re.fullmatch(r'promotions/(\d{4}-\d{2}-\d{2})-(.*)', self._branch)
        assert m is not None, R(
            'Cannot extract date and target from branch name', self._branch)
        return m.group(1), m.group(2)

    @attrs.frozen
    class _IssueInfo:
        title: str
        type: str
        url: str

        @property
        def ref(self) -> str:
            segments = furl(self.url).path.segments
            owner, repo, _, number = segments[0], segments[1], segments[2], segments[3]
            if repo == 'azul':
                return f'#{number}'
            else:
                return f'{owner}/{repo}#{number}'

    def _issue_info(self, issue_number: int) -> _IssueInfo:
        repo_queries = '\n'.join(
            fd(
                '''
                {alias}: repository(owner: "{owner}", name: "{repo}") {{
                    issue(number: {number}) {{
                        title
                        url
                        state
                        issueType {{ name }}
                    }}
                }}
                ''',
                alias=repo.replace('-', '_'),
                owner=_project_owner,
                repo=repo,
                number=issue_number
            )
            for repo in _project_repos
        )
        result = subprocess.run(
            [
                'gh', 'api', 'graphql',
                '-f', 'query={ ' + repo_queries + ' }',
            ],
            capture_output=True, text=True
        )
        response = json.loads(result.stdout)
        data = response['data']
        issues = [
            repo_data['issue']
            for repo_data in data.values()
            if repo_data['issue'] is not None
        ]
        open_issues = [i for i in issues if i['state'] == 'OPEN']
        if len(open_issues) == 1:
            issue = open_issues[0]
        elif len(open_issues) > 1:
            assert False, R(
                'Multiple open issues with the same number',
                [i['url'] for i in open_issues])
        elif len(issues) == 1:
            issue = issues[0]
        elif len(issues) > 1:
            assert False, R(
                'Multiple closed issues with the same number',
                [i['url'] for i in issues])
        else:
            assert False, R(
                'Issue not found in any repository', issue_number)
        issue_type = issue['issueType']
        return self._IssueInfo(
            title=issue['title'],
            type=issue_type['name'] if issue_type else '',
            url=issue['url']
        )

    @cached_property
    def _pr_title(self) -> str:
        prefix = 'Fix: ' if self._fix else ''
        suffix = self._title_suffix
        issue_refs = ', '.join(self._issue_refs)
        return f'{prefix}{self._issue.title}{suffix} ({issue_refs})'

    @cached_property
    def _existing_pr(self) -> JSON | None:
        log.info('Checking for existing PR …')
        result = subprocess.run(
            ['gh', 'pr', 'view', '--json', 'url,body,baseRefName'],
            capture_output=True, text=True
        )
        if result.returncode != 0:
            return None
        return json.loads(result.stdout)

    def _add_issue_references_to_body(self) -> None:
        self.body, n = re.subn(r'^(Linked issues?: *).*$',
                               lambda m: m.group(1) + ', '.join(self._issue_refs),
                               self.body, flags=re.MULTILINE)
        assert n > 0, R('Linked issues reference not found in body')
        assert n < 2, R('Multiple linked issues references found in body')

    def _reindex_labels(self) -> list[str]:
        return re.findall(
            r'^- \[[ x]] ' + _item_label + r'This PR is labeled `(reindex:\w+)`',
            self.body,
            flags=re.MULTILINE
        )

    def _mirror_labels(self) -> list[str]:
        return re.findall(
            r'^- \[[ x]] ' + _item_label + r'This PR is labeled `(mirror:\w+)`',
            self.body,
            flags=re.MULTILINE
        )

    def _deploy_labels(self) -> list[str]:
        return re.findall(
            r'^- \[[ x]] ' + _item_label + r'This PR is labeled `(deploy:\w+)`',
            self.body,
            flags=re.MULTILINE
        )

    def _check_task(self, task: str, checked: bool = True) -> None:
        mark = 'x' if checked else ' '
        self.body, n = re.subn(r'^- \[[ x]] (' + _item_label + task + ')$',
                               r'- [' + mark + r'] \1',
                               self.body, flags=re.MULTILINE)
        assert n > 0, R('Task item not found in template', task)
        assert n < 2, R('Multiple matching task items found', task)

    @cached_property
    def _branch_handle(self) -> str:
        m = re.fullmatch(r'issues/([^/]+)/\d+-.*', self._branch)
        assert m is not None, R('Cannot extract handle from branch name', self._branch)
        return m.group(1)

    def _github_user(self) -> str:
        result = subprocess.run(
            ['gh', 'api', 'user', '--jq', '.login'],
            capture_output=True, text=True, check=True
        )
        return result.stdout.strip()

    def _gh_item_type(self, url: str) -> str:
        path_kind = furl(url).path.segments[2]
        result = {'pull': 'pr', 'issues': 'issue'}.get(path_kind)
        assert result is not None, R('Cannot determine issue or PR from URL', url)
        return result

    def _node_id(self, url: str) -> str:
        item_type = self._gh_item_type(url)
        result = subprocess.run(
            ['gh', item_type, 'view', url, '--json', 'id', '--jq', '.id'],
            capture_output=True, text=True, check=True
        )
        return result.stdout.strip()

    def _link_issue(self, issue_node_id: str, pr_node_id: str) -> None:
        query = fd('''
            mutation {{
                addCloseIssueReferences(input: {{
                    issueId: "{issue_id}",
                    pullRequestIds: ["{pr_id}"]
                }}) {{ issue {{ id }} }}
            }}
        ''', issue_id=issue_node_id, pr_id=pr_node_id)
        subprocess.run(
            ['gh', 'api', 'graphql', '-f', f'query={query}'],
            capture_output=True, text=True, check=True
        )

    def _set_status(self, node_id: str, status: str) -> None:
        query = fd('''
            mutation {{
                addProjectV2ItemById(input: {{
                    projectId: "{project_id}",
                    contentId: "{node_id}"
                }}) {{ item {{ id }} }}
            }}
        ''', project_id=self._project_id, node_id=node_id)
        result = subprocess.run(
            ['gh', 'api', 'graphql', '-f', f'query={query}'],
            capture_output=True, text=True, check=True
        )
        result = json.loads(result.stdout)
        item_id = result['data']['addProjectV2ItemById']['item']['id']

        option_id = self._status_option_id(status)

        query = fd('''
            mutation {{
                updateProjectV2ItemFieldValue(input: {{
                    projectId: "{project_id}",
                    itemId: "{item_id}",
                    fieldId: "{field_id}",
                    value: {{singleSelectOptionId: "{option_id}"}}
                }}) {{ projectV2Item {{ id }} }}
            }}
        ''', project_id=self._project_id, item_id=item_id,
                   field_id=self._status_field['id'], option_id=option_id)
        subprocess.run(
            ['gh', 'api', 'graphql', '-f', f'query={query}'],
            capture_output=True, text=True, check=True
        )

    @cached_property
    def _project_id(self) -> str:
        query = fd('''
            {{
                organization(login: "{owner}") {{
                    projectV2(number: {number}) {{ id }}
                }}
            }}
        ''', owner=_project_owner, number=self._project['number'])
        result = subprocess.run(
            ['gh', 'api', 'graphql', '-f', f'query={query}'],
            capture_output=True, text=True, check=True
        )
        return json.loads(result.stdout)['data']['organization']['projectV2']['id']

    @cached_property
    def _status_field(self) -> dict:
        result = subprocess.run(
            [
                'gh', 'project', 'field-list', str(self._project['number']),
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

    def _status_option_id(self, status: str) -> str:
        for option in self._status_field['options']:
            if option['name'] == status:
                return option['id']
        assert False, R('Status option not found', status)

    @cached_property
    def _project(self) -> dict:
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
    Main(sys.argv[1:]).main()
