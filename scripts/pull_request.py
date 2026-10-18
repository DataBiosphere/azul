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


class Main:
    project_owner = 'DataBiosphere'
    project_repos = ['azul', 'azul-private']
    project_title = 'Azul'

    #: Matches the label that prefixes every checklist item in the PR templates
    #:
    item_label = r'`[A-Z]\d\d` '

    @classmethod
    def parse_args(cls, argv: list[str]) -> Namespace:
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
            'partial': 'Remove partial label and check partiality items.',
            'mirror': 'Remove mirror labels and check mirror items.',
            'reindex': 'Remove reindex labels and check reindex items.',
            'api': 'Remove API label and check API items.',
            'upgrade': 'Remove upgrade label and check upgrade items.',
            'deploy': 'Remove deploy labels and check deploy items.',
            'hotfix': 'Check hotfix items.',
            'reqs': 'Remove reqs label and check requirements items.',
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
        args = parser.parse_args(argv)
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

    def __init__(self, argv: list[str]):
        self.args = self.parse_args(argv)
        self._body: str | None = None
        self.labels_to_add: list[str] = []
        self.labels_to_remove: list[str] = []
        self.verify_working_copy()
        self.verify_remote_branch()
        self.verify_user()

    def main(self):
        self.handle_issue_references_in_body()
        self.handle_target_branch()
        self.handle_partiality()
        self.handle_upgrades()
        self.handle_deploy()
        self.handle_reindex()
        self.handle_mirror()
        self.handle_api()
        self.handle_hotfix()
        self.handle_reqs()

        self.check_item('PR is assigned to the author')
        self.check_item(r'Status of PR is \*In progress\*')
        self.check_item('Name of PR branch matches .*')

        self.check_item('PR is linked to .*')
        self.check_item(r'Status of linked issues? is \*In progress\*')
        self.check_item('PR description links to linked issues?')

        if self.existing_pr is None:
            pr_url = self.create_pr()
        else:
            pr_url = json_str(self.existing_pr['url'])
            self.update_pr(pr_url)

        log.info('Setting PR status …')
        pr_node_id = self.node_id(pr_url)
        self.set_status(pr_node_id, 'In Progress')

        for issue in self.issues:
            issue_node_id = self.node_id(issue.url)
            log.info('Linking PR to issue %s …', issue.ref)
            self.link_issue(issue_node_id, pr_node_id)
            log.info('Setting status of issue %s …', issue.ref)
            self.set_status(issue_node_id, 'In Progress')

    def verify_working_copy(self) -> None:
        result = subprocess.run(
            ['git', 'status', '--porcelain'],
            capture_output=True, text=True, check=True
        )
        if result.stdout.strip():
            log.warning('Working copy has uncommitted changes')

    def verify_remote_branch(self) -> None:
        log.info('Checking remote branch …')
        result = subprocess.run(
            ['git', 'ls-remote', '--heads', 'github', self.branch],
            capture_output=True, text=True, check=True
        )
        assert result.stdout.strip(), R(
            'Branch does not exist on the github remote', self.branch)

        remote_sha = result.stdout.split()[0]
        local_sha = subprocess.run(
            ['git', 'rev-parse', 'HEAD'],
            capture_output=True, text=True, check=True
        ).stdout.strip()

        if local_sha != remote_sha:
            subprocess.run(
                ['git', 'fetch', 'github', self.branch],
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

    def verify_user(self):
        if self.args.type is None:
            m = re.fullmatch(r'issues/([^/]+)/\d+-.*', self.branch)
            assert m is not None, R(
                'Cannot extract handle from branch name', self.branch)
            handle = m.group(1)
            log.info('Verifying GitHub user …')
            result = subprocess.run(
                ['gh', 'api', 'user', '--jq', '.login'],
                capture_output=True, text=True, check=True
            )
            assert handle == result.stdout.strip(), R(
                'Branch name does not match GitHub user', handle)

    def handle_issue_references_in_body(self) -> None:
        self.body, n = re.subn(
            r'^(Linked issues?: *).*$',
            lambda m: m.group(1) + ', '.join(self.issue_refs),
            self.body, flags=re.MULTILINE
        )
        assert n > 0, R('Linked issues reference not found in body')
        assert n < 2, R('Multiple linked issues references found in body')

    def handle_target_branch(self):
        item = r'Target branch is `' + re.escape(self.target_branch) + '`'
        if self.existing_pr is None:
            self.check_item(item)
        else:
            base = self.existing_pr['baseRefName']
            if base == self.target_branch:
                self.check_item(item)
            else:
                log.warning('Target branch is %r, expected %r', base, self.target_branch)
                self.check_item(item, checked=False)

    def handle_partiality(self):
        if self.args.no_partial:
            assert not self.has_commit_tag('p'), R(
                '--no-partial cannot be used when a commit is tagged `p`')
            self.check_item(r'Added `p` tag to titles of partial commits.*')
            self.check_item(r'This PR is labeled `partial`.*')
            self.check_item(r'This PR partially resolves .*')
            self.labels_to_remove.append('partial')

    def handle_upgrades(self):
        has_u_tag = self.has_commit_tag('u')
        assert not (has_u_tag and self.args.no_upgrade), R(
            '--no-upgrade cannot be used when a commit is tagged `u`')
        checked = has_u_tag or self.args.no_upgrade
        if self.args.type != 'promotion':
            self.check_item(r'Added `u` tag to commit title.*', checked=checked)
        self.check_item(r'This PR is labeled `upgrade`.*', checked=checked)
        if has_u_tag:
            self.labels_to_add.append('upgrade')
        else:
            self.labels_to_remove.append('upgrade')
        if self.args.no_upgrade:
            self.check_item(r'Ran `make docker_images\.json`.*')
            self.check_item(r'Documented upgrading of deployments in UPGRADING\.rst.*')

    def handle_deploy(self):
        if self.args.no_deploy:
            deploy_labels = self.labels('deploy')
            for label in deploy_labels:
                self.check_item(r'This PR is labeled `' + re.escape(label) + '`.*')
            self.labels_to_remove.extend(deploy_labels)

    def handle_reindex(self):
        if self.args.no_reindex:
            assert not self.has_commit_tag('r'), R(
                '--no-reindex cannot be used when a commit is tagged `r`')
            self.check_item(r'Added `r` tag to commit title.*')
            reindex_labels = self.labels('reindex')
            for label in reindex_labels:
                self.check_item(r'This PR is labeled `' + re.escape(label) + '`.*')
            self.labels_to_remove.extend(reindex_labels)

    def handle_mirror(self):
        if self.args.no_mirror:
            mirror_labels = self.labels('mirror')
            for label in mirror_labels:
                self.check_item(r'This PR is labeled `' + re.escape(label) + '`.*')
            self.labels_to_remove.extend(mirror_labels)

    def handle_api(self):
        if self.args.no_api:
            assert not self.has_commit_tag('a'), R(
                '--no-api cannot be used when a commit is tagged `a`')
            assert not self.has_commit_tag('A'), R(
                '--no-api cannot be used when a commit is tagged `A`')
            self.check_item(r'This PR and its linked issues are labeled `API`.*')
            self.check_item(r'Added `a` \(`A`\) tag to commit title.*')
            self.check_item(r'Updated REST API version number in `app\.py`.*')
            self.labels_to_remove.append('API')

    def handle_hotfix(self):
        if self.args.no_hotfix:
            assert not self.has_commit_tag('F'), R(
                '--no-hotfix cannot be used when a commit is tagged `F`')
            self.check_item(r'Added `F` tag to main commit title.*')
            self.check_item(r'Reverted the temporary hotfixes for any linked issues.*')

    def handle_reqs(self):
        if self.args.no_reqs:
            assert not self.has_commit_tag('R'), R(
                '--no-reqs cannot be used when a commit is tagged `R`')
            self.check_item(r'Ran `make requirements_update`.*')
            self.check_item(r'Added `R` tag to commit title.*')
            self.check_item(r'This PR is labeled `reqs`.*')
            self.labels_to_remove.append('reqs')

    @cached_property
    def existing_pr(self) -> JSON | None:
        log.info('Checking for existing PR …')
        result = subprocess.run(
            ['gh', 'pr', 'view', '--json', 'url,body,baseRefName'],
            capture_output=True, text=True
        )
        if result.returncode != 0:
            return None
        return json.loads(result.stdout)

    def create_pr(self) -> str:
        log.info('Creating PR …')
        cmd = [
            'gh', 'pr', 'create',
            '--base', self.target_branch,
            '--title', self.pr_title,
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

    def update_pr(self, pr_url: str) -> None:
        log.info('Updating PR …')
        cmd = [
            'gh', 'pr', 'edit', pr_url,
            '--title', self.pr_title,
            '--body', self.body,
            '--add-assignee', '@me',
        ]
        if self.labels_to_add:
            cmd.extend(['--add-label', ','.join(self.labels_to_add)])
        if self.labels_to_remove:
            cmd.extend(['--remove-label', ','.join(self.labels_to_remove)])
        subprocess.run(cmd, capture_output=True, text=True, check=True)
        log.info('PR URL is %r', pr_url)

    def check_item(self, item: str, checked: bool = True) -> None:
        mark = 'x' if checked else ' '
        self.body, n = re.subn(
            r'^- \[[ x]] (' + self.item_label + item + ')$',
            r'- [' + mark + r'] \1',
            self.body,
            flags=re.MULTILINE
        )
        assert n > 0, R('Item not found in template', item)
        assert n < 2, R('Multiple matching items found', item)

    def labels(self, prefix: str) -> list[str]:
        item = r'This PR is labeled `(' + prefix + r':\w+)`'
        return re.findall(
            r'^- \[[ x]] ' + self.item_label + item,
            self.body,
            flags=re.MULTILINE
        )

    def node_id(self, url: str) -> str:
        item_type = self.gh_item_type(url)
        result = subprocess.run(
            ['gh', item_type, 'view', url, '--json', 'id', '--jq', '.id'],
            capture_output=True, text=True, check=True
        )
        return result.stdout.strip()

    def gh_item_type(self, url: str) -> str:
        path_kind = furl(url).path.segments[2]
        result = {'pull': 'pr', 'issues': 'issue'}.get(path_kind)
        assert result is not None, R('Cannot determine issue or PR from URL', url)
        return result

    def set_status(self, node_id: str, status: str) -> None:
        query = fd('''
            mutation {{
                addProjectV2ItemById(input: {{
                    projectId: "{project_id}",
                    contentId: "{node_id}"
                }}) {{ item {{ id }} }}
            }}
        ''', project_id=self.project_id, node_id=node_id)
        result = subprocess.run(
            ['gh', 'api', 'graphql', '-f', f'query={query}'],
            capture_output=True, text=True, check=True
        )
        result = json.loads(result.stdout)
        item_id = result['data']['addProjectV2ItemById']['item']['id']

        option_id = self.status_option_id(status)

        query = fd('''
            mutation {{
                updateProjectV2ItemFieldValue(input: {{
                    projectId: "{project_id}",
                    itemId: "{item_id}",
                    fieldId: "{field_id}",
                    value: {{singleSelectOptionId: "{option_id}"}}
                }}) {{ projectV2Item {{ id }} }}
            }}
        ''', project_id=self.project_id, item_id=item_id,
                   field_id=self.status_field['id'], option_id=option_id)
        subprocess.run(
            ['gh', 'api', 'graphql', '-f', f'query={query}'],
            capture_output=True, text=True, check=True
        )

    @cached_property
    def project_id(self) -> str:
        query = fd('''
            {{
                organization(login: "{owner}") {{
                    projectV2(number: {number}) {{ id }}
                }}
            }}
        ''', owner=self.project_owner, number=self.project['number'])
        result = subprocess.run(
            ['gh', 'api', 'graphql', '-f', f'query={query}'],
            capture_output=True, text=True, check=True
        )
        return json.loads(result.stdout)['data']['organization']['projectV2']['id']

    def status_option_id(self, status: str) -> str:
        for option in self.status_field['options']:
            if option['name'] == status:
                return option['id']
        assert False, R('Status option not found', status)

    @cached_property
    def status_field(self) -> dict:
        result = subprocess.run(
            [
                'gh', 'project', 'field-list', str(self.project['number']),
                '--owner', self.project_owner,
                '--format', 'json',
            ],
            capture_output=True, text=True, check=True
        )
        fields = json.loads(result.stdout)['fields']
        for field in fields:
            if field['name'] == 'Status':
                return field
        assert False, R('Status field not found in project')

    @cached_property
    def project(self) -> dict:
        result = subprocess.run(
            [
                'gh', 'project', 'list',
                '--owner', self.project_owner,
                '--format', 'json',
            ],
            capture_output=True, text=True, check=True
        )
        projects = json.loads(result.stdout)['projects']
        for project in projects:
            if project['title'] == self.project_title:
                return project
        assert False, R('Project not found', self.project_title)

    @cached_property
    def issues(self) -> list[IssueInfo]:
        issue_numbers = self.issue_numbers
        log.info('Fetching issue %s …',
                 join_grammatically([f'#{n}' for n in issue_numbers]))
        return [self.issue_info(issue_number) for issue_number in issue_numbers]

    def link_issue(self, issue_node_id: str, pr_node_id: str) -> None:
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

    @property
    def body(self) -> str:
        if self._body is None:
            if self.existing_pr is None:
                self._body = self.template
            else:
                self._body = json_str(self.existing_pr['body'])
                expected_comment = self.template.split('-->', maxsplit=1)[0]
                assert self._body.startswith(expected_comment), R(
                    'Existing PR was created with a different template')
            # Normalize line endings from GitHub API responses
            self._body = '\n'.join(self._body.splitlines())
        return self._body

    @body.setter
    def body(self, body: str) -> None:
        self._body = body

    @cached_property
    def target_branch(self) -> str:
        m = re.search(
            r'^- \[[ x]] ' + self.item_label + r'Target branch is `(.+?)`$',
            self.template,
            flags=re.MULTILINE
        )
        assert m is not None, R('Target branch item not found in template')
        return m.group(1)

    @cached_property
    def template(self) -> str:
        return self.template_path.read_text()

    @cached_property
    def template_path(self) -> Path:
        project_root = Path(__file__).resolve().parent.parent
        github_dir = project_root / '.github'
        template_dir = github_dir / 'PULL_REQUEST_TEMPLATE'
        if self.args.type is None:
            template_path = github_dir / 'pull_request_template.md'
        elif self.args.type == 'upgrade':
            template_path = template_dir / 'upgrade.md'
        elif self.args.type == 'promotion':
            date, target = self.promotion_date_and_target
            template_path = template_dir / f'{target}-promotion.md'
        else:
            assert False, R('Unsupported template', self.args.type)
        return template_path

    @cached_property
    def pr_title(self) -> str:
        prefix = 'Fix: ' if self.fix else ''
        suffix = self.title_suffix
        issue_refs = ', '.join(self.issue_refs)
        return f'{prefix}{self.issue.title}{suffix} ({issue_refs})'

    @cached_property
    def fix(self) -> bool:
        if self.args.fix is None:
            return self.issue.type == 'Defect'
        else:
            return self.args.fix

    @cached_property
    def title_suffix(self) -> str:
        if self.args.type == 'promotion':
            date, target = self.promotion_date_and_target
            title_suffix = f' {target}'
        else:
            title_suffix = ''
        return title_suffix

    @cached_property
    def issue(self) -> IssueInfo:
        # The PR is assumed to resolve every one of the linked issues. The first
        # of them determines the PR title.
        return self.issues[0]

    @cached_property
    def issue_refs(self) -> list[str]:
        return [issue.ref for issue in self.issues]

    @cached_property
    def issue_numbers(self) -> list[int]:
        issue_numbers = self.args.issues
        if issue_numbers is None:
            if self.args.type is None:
                issue_numbers = self.issue_numbers_from_branch_name
            elif self.args.type == 'upgrade':
                date = self.upgrade_date
                log.info('Searching for upgrade issue …')
                issue_numbers = [self.issue_number_by_title(
                    f'Upgrade software dependencies {date}'
                )]
            else:
                date, target = self.promotion_date_and_target
                log.info('Searching for promotion issue …')
                issue_numbers = [self.issue_number_by_title(
                    f'Promotion {date}'
                )]
        return issue_numbers

    def issue_number_by_title(self, title: str) -> int:
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

    @attrs.frozen
    class IssueInfo:
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

    def issue_info(self, issue_number: int) -> IssueInfo:
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
                owner=self.project_owner,
                repo=repo,
                number=issue_number
            )
            for repo in self.project_repos
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
        return self.IssueInfo(
            title=issue['title'],
            type=issue_type['name'] if issue_type else '',
            url=issue['url']
        )

    @cached_property
    def issue_numbers_from_branch_name(self) -> list[int]:
        m = re.fullmatch(r'issues/[^/]+/(\d+(?:-\d+)*)-.*', self.branch)
        assert m is not None, R(
            'Cannot extract issue numbers from branch name', self.branch)
        return [int(number) for number in m.group(1).split('-')]

    @cached_property
    def upgrade_date(self) -> str:
        m = re.fullmatch(r'upgrades/(\d{4}-\d{2}-\d{2})', self.branch)
        assert m is not None, R('Cannot extract date from branch name', self.branch)
        return m.group(1)

    @cached_property
    def promotion_date_and_target(self) -> tuple[str, str]:
        m = re.fullmatch(r'promotions/(\d{4}-\d{2}-\d{2})-(.*)', self.branch)
        assert m is not None, R(
            'Cannot extract date and target from branch name', self.branch)
        return m.group(1), m.group(2)

    @cached_property
    def branch(self) -> str:
        result = subprocess.run(
            ['git', 'rev-parse', '--abbrev-ref', 'HEAD'],
            capture_output=True, text=True, check=True
        )
        return result.stdout.strip()

    def has_commit_tag(self, tag: str) -> bool:
        result = subprocess.run(
            ['git', 'log', '--format=%s', f'{self.target_branch}..HEAD'],
            capture_output=True, text=True, check=True
        )
        return any(
            tag in self.commit_title_tags(title)
            for title in result.stdout.splitlines()
        )

    def commit_title_tags(self, title: str) -> set[str]:
        m = re.match(r'^\[([^]]*)]', title)
        if m is None:
            return set()
        else:
            return set(m.group(1).split())


if __name__ == '__main__':
    configure_script_logging(log)
    Main(sys.argv[1:]).main()
