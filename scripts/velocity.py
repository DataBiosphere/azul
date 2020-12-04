"""
Generates reports on recently-closed issues on ZenHub and GitHub.

To run this script locally, printing the report to stdout, the following
variables must be configured locally, preferably in a `environment.local.py`:

* azul_velocity_github_token

* azul_velocity_zenhub_key

* azul_velocity_zenhub_pkey

* azul_velocity_zenhub_url

Instructions on how to configure these variables (including required scopes,
etc.) are listed in `environment.py` at the project root.

To run this script with GitHub Actions, the aforementioned variables must be
defined as secrets. For more info on setting secrets, see
https://docs.github.com/en/actions/configuring-and-managing-workflows/creating-and-storing-encrypted-secrets.

To publish the generated report to a Gitlab wiki, using GitHub Actions, the
`gitlab_api_key` secret must also be set to a Gitlab personal access token with
the `api` scope.

See `.github/workflows/velocity.yml` for an example workflow publishing a
velocity report to a Gitlab wiki.
"""

import argparse
import collections
from datetime import (
    datetime,
    timedelta,
)
from email.utils import (
    parsedate_to_datetime,
)
import itertools
import logging
import os
import time
from typing import (
    Any,
    Iterator,
    Mapping,
    Optional,
    Sequence,
    Tuple,
)
import urllib.parse

import github
import github.Issue
import github.Requester
import requests

from azul import (
    cached_property,
    lru_cache,
)
from azul.json import (
    JSON,
)
from azul.logging import (
    configure_script_logging,
)

log = logging.getLogger(__name__)
config = {}


def report(since: datetime, until: datetime):
    issues, prs = fetch_issues(since, until)
    review_time = collections.Counter()
    review_lines = collections.Counter()
    story_points = collections.Counter()
    for issue in issues:
        try:
            story_points[issue.assignee.login] += issue.spike_estimate
        except (TypeError, AttributeError):  # assignee or spike estimate can be None
            pass
    for pr in prs:
        if pr.merged:
            try:
                story_points[pr.user.login] += pr.linked_estimate
            except TypeError:
                pass
        review_time.update(pr.reviewer_time)
        review_lines.update({r: pr.lines_changed for r in pr.reviewer_time})

    contrib_table = []
    points = {
        user: (story_points.get(user, 0), review_time.get(user, 0))
        for user in (review_time + story_points)
    }
    for contributor, total in points.items():
        _story_points, _review_points = total
        user = f'[@{contributor}](https://github.com/{contributor})'
        contrib_table.append([user, _story_points, _review_points, sum(total)])
    story_sum = sum(story_points.values())
    review_sum = sum(review_time.values())
    contrib_table.append(['Total', story_sum, review_sum, story_sum + review_sum])

    pr_table = []
    for reviewer, hours in review_time.items():
        user = f'[@{reviewer}](https://github.com/{reviewer})'
        ratio = f'{(hours * 60 * 60) / review_lines[reviewer]:.3f}'
        pr_table.append([user, hours, review_lines[reviewer], ratio])

    spiked_issues = [issue for issue in issues if issue.spike_estimate]
    spiked_table = []
    for issue in spiked_issues:
        spike = issue.spike_estimate if issue.spike_estimate is not None else 0
        estimate = issue.estimate if issue.estimate is not None else 0
        spiked_table.append([issue.assignees_html(),
                             issue.markdown(),
                             issue.spike_estimate,
                             issue.estimate,
                             spike + estimate])

    unspiked_issues = [issue for issue in issues if not issue.spike_estimate]
    issue_table = []
    for issue in unspiked_issues:
        issue_table.append([issue.assignees_html(), issue.markdown(), issue.estimate])

    merged = [p for p in prs if p.merged]
    closed = [p for p in prs if not p.merged]

    print(f'''\
# {config['workspace']!r} velocity report for {_human_daterange(since, until)}
This report covers the period between {since.strftime('%c')} and \
{until.strftime('%c')}.

## Summary
In this reporting period, {len(prs)} pull requests and {len(issues)} issues \
were closed. Together, these accounted for \
{sum(filter(None, (i.estimate for i in [*issues, *prs])))} story points.

According to ZenHub, {len(review_time)} reviewers updated estimates based on \
time taken to review code.

## Story points
Points are awarded to pull request authors and to issue assignees on merged \
and closed issues and pull requests.

| Contributor | Issue points | Review points | Total |
| ----------- | :----------: | :-----------: | :---: |
{_table(*contrib_table)}

## Pull requests

At the time this report was generated, {len(prs)} were marked merged or \
closed in the reporting period. Not all closed PRs are merged; some PRs \
may have been superseded by another PR and thus closed without merging.

| Reviewer | Time spent | Lines reviewed | Ratio (sec/line) |
| -------- | :--------: | :------------: | :--------------: |
{_table(*pr_table)}

At the time this report was generated, {len(merged)} pull requests were merged:

| Author | Pull request | Linked estimate | Review cycles | Lines changed | Ratio |
| ------ | ------------ | :-------------: | :-----------: | :-----------: | :---: |
{_table(*pr_rows(*merged))}

Of the {len(prs)} pull requests closed in this reporting period, {len(closed)} \
pull requests were closed but not merged. Points for these PRs are not \
counted towards contributor totals above.

| Author | Pull request | Linked estimate | Review cycles | Lines changed | Ratio |
| ------ | ------------ | :-------------: | :-----------: | :-----------: | :---: |
{_table(*pr_rows(*closed))}

## Resolved issues
At the time this report was generated, {len(issues)} were marked closed in \
the reporting period. This indicates that the associated work is considered \
to have "landed" and been demoed.

| Assignees | Issue | Estimate |
| --------- | ----- | :------: |
{_table(*issue_table)}

### Spiked
Of the above {len(issues)} issues, {len(spiked_issues)} issues were marked \
with a separate spike estimate:

| Assignees | Issue | Spike estimate | Issue estimate | Total |
| --------- | ----- | :------------: | :------------: | :---: |
{_table(*spiked_table)}''')


def _table(*rows: Sequence) -> str:
    """
    >>> _table(['a', 1, 'c', 1.23])
    '|a|1|c|1.23|'
    >>> _table(['foo', 0, None])
    '|foo|||'
    >>> print(_table(['multiple', 'rows', 0], ['each', 'a', 'list']))
    |multiple|rows||
    |each|a|list|
    """
    return '\n'.join([
        '|'.join(['', *[str(cell or '') for cell in row], '']) for row in rows
    ])


@lru_cache
def get_user_by_id(user_id: int) -> str:
    # ZenHub identifies users only by their GitHub user ID. The only way to map
    # a user ID to a user name is to use an undocumented API endpoint that
    # PyGithub does not directly support. (PyGithub/PyGithub#1615)
    r = requests.get(f'https://api.github.com/user/{user_id}',
                     auth=(config['github_user'], config['github_key']))
    r.raise_for_status()
    return r.json()['login']


def _human_daterange(beginning: datetime, end: datetime) -> str:
    """
    >>> _human_daterange(datetime.now() - timedelta(days=25), datetime.now())
    'the last 3 weeks and 4 days'
    >>> _human_daterange(datetime.now() - timedelta(days=10), datetime.now())
    'the last 1 week and 3 days'
    >>> _human_daterange(datetime.now() - timedelta(days=8), datetime.now())
    'the last 1 week and 1 day'
    """
    days = (end - beginning).days
    week_word = 'weeks' if days > 13 else 'week'
    days_word = 'days' if days % 7 > 1 else 'day'
    if days == 1:
        return 'yesterday'
    elif days < 7:
        return f'the last {days} {days_word}'
    elif days == 7:
        return 'the last week'
    elif days % 7 == 0:
        return f'the last {int(days / 7)} {week_word}'
    else:  # days % 7 > 0
        return f'the last {days // 7} {week_word} and {days % 7} {days_word}'


class ZenHubAPI(requests.Session):

    def __init__(self,
                 api_key: str,
                 private_api_key: str,
                 base_url: str = 'https://api.zenhub.com/'):
        super(ZenHubAPI, self).__init__()
        self.headers.update({'X-Authentication-Token': api_key})
        self.private_api_key = private_api_key
        self.hooks['response'] = [self._save_ratelimit_headers]
        self.base_url = base_url
        self.remaining = 0
        self.reset_time = 0

    def _save_ratelimit_headers(self, response: requests.Response, *_, **__):
        """
        Called when a response is generated from a :class:`requests.Request`.
        Saves rate limiting header information used by :meth:`request`.

        See https://github.com/ZenHubIO/API#api-rate-limit
        """
        limit = int(response.headers['X-RateLimit-Limit'])
        used = int(response.headers['X-RateLimit-Used'])
        self.remaining = limit - used
        # ZenHub API documentation explicitly recommends using "their" time
        # as indicated in the Date header
        current_time = parsedate_to_datetime(response.headers['Date']).timestamp()
        self.reset_time = current_time - int(response.headers['X-RateLimit-Reset'])

    def request(self, method, url, *args, **kwargs) -> requests.Response:
        url = urllib.parse.urljoin(self.base_url, url)
        if self.remaining == 0:
            # self.reset_time is sometimes negative, which makes no sense
            # since ZH defines both operands
            time.sleep(max(self.reset_time, 5))
        return super(ZenHubAPI, self).request(method, url, *args, **kwargs)

    @lru_cache
    def get_issue(self, repo_id: int, issue_num: int) -> requests.Response:
        return self.get(f'/p1/repositories/{repo_id}/issues/{issue_num}')

    def get_issue_history(self, repo_id: int, issue_num: int) -> requests.Response:
        return self.get(f'/p1/repositories/{repo_id}/issues/{issue_num}/events')

    def get_issue_history_private(self,
                                  repo_id: int,
                                  issue_num: int) -> requests.Response:
        # ZenHub exposes issue-PR relationships only thru their private API
        path = f'/v5/repositories/{repo_id}/issues/{issue_num}/events'
        url = urllib.parse.urljoin(self.base_url, path)
        r = requests.get(url, headers={'X-Authentication-Token': self.private_api_key})
        r.raise_for_status()
        return r


class Issue(github.Issue.Issue):

    def __init__(self, issue: github.Issue.Issue, zenhub: ZenHubAPI):
        super().__init__(issue._requester, issue._headers, issue._rawData, True)
        self._zenhub = zenhub

    @property
    def label_names(self) -> Sequence[str]:
        return [label.name for label in self.labels]

    @property
    def zenhub_issue(self) -> JSON:
        return self._zenhub.get_issue(self.repository.id, self.number).json()

    @property
    def estimate(self) -> Optional[int]:
        try:
            return int(self.zenhub_issue['estimate']['value'])
        except KeyError:
            return None

    def __str__(self) -> str:
        return f'{self.repository.full_name}#{self.number}: {self.title}'

    def markdown(self) -> str:
        return f'[{self.repository.full_name}#{self.number}]({self.html_url})' + \
               f': {self.title}'

    def assignees_html(self) -> str:
        return ', '.join([f'[@{a.login}]({a.html_url})' for a in self.assignees])

    @property
    def spike_estimate(self) -> Optional[int]:
        spike_labels = [label for label in self.label_names
                        if label.startswith('spike:')]
        if len(spike_labels) > 1:
            raise RuntimeError(f"Multiple spike labels exist for {str(self)}")
        else:
            try:
                return int(spike_labels[0].lstrip('spike:'))
            except IndexError:
                return None


class PullRequest(Issue):

    def __init__(self, issue: github.Issue.Issue, zenhub: ZenHubAPI):
        super().__init__(issue, zenhub)
        pr = issue.as_pull_request()
        self.deletions = pr.deletions
        self.additions = pr.additions
        self.merged = pr.merged

    @property
    def lines_changed(self) -> int:
        return self.additions + self.deletions

    @property
    def reviews(self) -> Optional[int]:
        review_labels = sorted(label for label in self.label_names
                               if 'review' in label)
        try:
            return int(review_labels[-1].rstrip('+ reviews'))
        except IndexError:
            return None

    @property
    def review_to_change_ratio(self) -> Optional[float]:
        try:
            return self.reviews / self.lines_changed
        except (ZeroDivisionError, TypeError):
            return None

    @cached_property
    def reviewer_time(self) -> collections.Counter:
        r = self._zenhub.get_issue_history(self.repository.id, self.number)
        events = (event for event in r.json() if event['type'] == 'estimateIssue')
        estimates = collections.Counter()
        for event in events:
            try:
                to_estimate = event['to_estimate']['value']
            except KeyError:
                to_estimate = 0
            try:
                from_estimate = event['from_estimate']['value']
            except KeyError:
                from_estimate = 0
            estimates[get_user_by_id(event['user_id'])] += to_estimate - from_estimate
        return estimates

    @cached_property
    def linked_estimate(self) -> Optional[int]:
        """
        If this pull request is linked to an issue in ZenHub, return
        the estimate of the issue in ZenHub. If not, return None.
        """
        events = self._zenhub.get_issue_history_private(self.repository.id, self.number)
        linked = set()
        for event in events.json()['events']:
            if event['type'] == 'connectPR':
                linked.add((event['other_issue']['repo_id'],
                            event['other_issue']['issue_number']))
            elif event['type'] == 'disconnectPR':
                linked.discard((event['other_issue']['repo_id'],
                                event['other_issue']['issue_number']))
        if len(linked) > 1:
            raise RuntimeError(f'Unexpected response from ZenHub for {self.html_url}')
        elif len(linked) == 0:
            return None
        elif len(linked) == 1:
            repo_id, issue_num = linked.pop()
            issue = self._zenhub.get_issue(repo_id, issue_num).json()
            try:
                return int(issue['estimate']['value'])
            except KeyError:
                return None


def pr_rows(*prs: PullRequest) -> Iterator:
    for pr in prs:
        author = f'[@{pr.user.login}]({pr.user.html_url})'
        row = [author, pr.markdown(), pr.linked_estimate, pr.reviews, pr.lines_changed]
        try:
            row.append(f'{pr.review_to_change_ratio:.3f}')
        except TypeError:
            row.append('')
        yield row


def fetch_issues(since: datetime,
                 until: datetime) -> Tuple[Sequence[Issue], Sequence[PullRequest]]:
    zh = ZenHubAPI(config['zenhub_key'], config['zenhub_pkey'])
    gh = github.Github(config['github_key'])
    closed_range = f'{since.strftime("%Y-%m-%d")}..{until.strftime("%Y-%m-%d")}'
    issues = itertools.chain.from_iterable((
        gh.search_issues('is:issue',
                         repo=gh.get_repo(int(repo)).full_name,
                         closed=closed_range)
        for repo in config['repos']
    ))
    prs = itertools.chain.from_iterable((
        gh.search_issues('is:pr',
                         repo=gh.get_repo(int(repo)).full_name,
                         closed=closed_range)
        for repo in config['repos']
    ))
    return [Issue(iss, zh) for iss in issues], [PullRequest(pr, zh) for pr in prs]


def parse_workspace_url(url: str) -> Mapping[str, Any]:
    """
    >>> workspace_url = 'https://app.zenhub.com/workspaces/foo-123/board?repos=456,789'
    >>> parse_workspace_url(workspace_url)
    {'workspace': 'foo', 'repos': ['456', '789']}
    """
    parsed = urllib.parse.urlparse(url)
    _, _workspaces, workspace_slug, _board = parsed.path.split('/')
    workspace_name, workspace_id = workspace_slug.split('-')
    return {
        'workspace': workspace_name,
        'repos': urllib.parse.parse_qs(parsed.query)['repos'][0].split(',')
    }


if __name__ == '__main__':
    configure_script_logging(log)
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    date_format = '%Y-%m-%d'
    parser.add_argument('--since',
                        default=(datetime.now() - timedelta(days=14)).strftime(date_format),
                        help='grab issues closed after this date, defaults to two weeks ago')
    parser.add_argument('--until',
                        default=datetime.now().strftime(date_format),
                        help='grab issues closed before this date, defaults to today')
    arguments = parser.parse_args()
    try:
        config = {
            'github_key': os.environ['azul_velocity_github_token'],
            'github_user': os.environ['azul_velocity_github_user'],
            'zenhub_key': os.environ['azul_velocity_zenhub_key'],
            'zenhub_pkey': os.environ['azul_velocity_zenhub_pkey'],
            **parse_workspace_url(os.environ['azul_velocity_zenhub_url'])
        }
    except KeyError:
        raise RuntimeError('Please run `source environment` from the project root, '
                           'and ensure that the azul_velocity_* envvars are configured.')
    else:
        report(since=datetime.strptime(arguments.since, date_format),
               until=datetime.strptime(arguments.until, date_format))
