import argparse
from datetime import (
    datetime,
    timedelta,
)
import json
import logging
from operator import (
    itemgetter,
)
import subprocess
import sys
import zoneinfo

tz = zoneinfo.ZoneInfo('America/Los_Angeles')

log = logging.getLogger('azul.github.schedule')

one_week = 7  # in days


def create_issue(gh_create_issue: list[str], gh_issue_lookup: list[str]) -> None:
    process = subprocess.run(gh_issue_lookup, check=True, stdout=subprocess.PIPE)
    results = json.loads(process.stdout)
    issues = set(map(itemgetter('number'), results))
    if issues:
        log.info('At least one matching issue already exists: %r', issues)
    else:
        subprocess.run(gh_create_issue, check=True)


def gh_commands(template: str,
                issue_date: datetime) -> tuple[list[str], list[str]]:
    template = f'.github/ISSUE_TEMPLATE/{template}'
    front_matter, body = _load_issue_template(template)
    labels, title, assignees = [front_matter[x]
                                for x in ('labels', 'title', 'assignees')]

    issue_date = issue_date.date()
    in_repository = []
    if template.endswith('fedramp_inventory_review.md'):
        in_repository = ['--repo DataBiosphere/' + front_matter['repository']]
    elif template.endswith('promotion.md'):
        # We want the Wendsday date promotion issue to be created on Tuesdays
        issue_date = issue_date + timedelta(days=1)
    if assignees:
        assignees = [f'--assignee {assignees}']
    return [
        'gh', 'issue', 'list',
        f'--search=in:title "{title} {issue_date}"',
        '--json=number',
        '--limit=10',
    ], [
        'gh', 'issue', 'create',
        *in_repository,
        *assignees,
        f'--title={title} {issue_date}',
        f'--label={labels}',
        f'--body={body}'
    ]


def _load_issue_template(path: str) -> tuple[dict[str, str], str]:
    """
    Load the issue template at the given path and parse any YAML front-matter
    embedded in the template. GitHub uses the front-matter in issue templates to
    allow for customizing issue properties other than the issue's description,
    such as its title.

    https://jekyllrb.com/docs/front-matter/

    :return: A tuple of front matter, a dictionary, and the body, a str. If
             there is no front-matter or if it is empty, the first element will
             be an empty dictionary
    """
    with open(path) as f:
        front_matter = {}
        line = f.readline()
        sep = '---\n'
        if line == sep:
            for line in f:
                if line == sep:
                    break
                else:
                    k, _, v = line.partition(':')
                    front_matter[k.strip()] = v.strip()
        else:
            f.seek(0)
        return front_matter, f.read()


def main(templates: list[tuple[str, datetime]], dry_run: bool) -> None:
    for template, create_date in templates:
        cmd_lookup, cmd_create = gh_commands(template, create_date)
        if dry_run:
            log.info('Would be running a mock command that creates an issue')
            print(' '.join(cmd_create))
        else:
            create_issue(cmd_create, cmd_lookup)


def _create(template: str,
            start: datetime,
            period_days: int,
            at_hour: int,
            ) -> tuple[str, datetime] | None:
    """
    Return a tuple of the template and the datetime.now() instance that matches the
    configured shcedule for the provided template. This logs the schedule check for
    each, and it only returns what falls within the schedule window.
    """
    log.info('Checking configured schedule time window for %r', template)
    if 0 == (now - start).days % period_days and now.hour == at_hour:
        log.info('Scheduling %r, it falls within schedule window', template)
        return template, now
    else:
        log.info('Current time is outside of the configured schedule window')


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s %(levelname)+7s %(name)s: %(message)s')

    args = sys.argv[1:]
    parser = argparse.ArgumentParser(
        description='Dryrun to display the command used by GH Actions to create an issue'
    )
    parser.add_argument('--dry-run',
                        metavar='MOCKED_DATE',
                        type=lambda s: datetime.strptime(s, '%Y-%m-%dT%H:%M'),
                        help='Dry run based on mock date to verify the template issue configuration')
    args = parser.parse_args(args)

    now = datetime.now(tz)
    if args.dry_run:
        dt = args.dry_run
        now = now.replace(year=dt.year,
                          month=dt.month,
                          day=dt.day,
                          hour=dt.hour,
                          minute=dt.minute)

    templates = [
        # Pick any date for `start` and an issue will be created on that date,
        # and thereafter based on the given period, but only in the future.
        # Note that this doesn't mean that the start date has to lie in the
        # future. As an example, …
        _create(template, start, period, hour) for template, start, period, hour in [
            (
                'upgrade.md',  # … the schedule for upgrade.md is set …
                datetime(2023, 11, 27, tzinfo=tz),  # … for Monday …
                one_week * 2,  # … every other week (14 days) …
                9  # … at 9am.
            ),
            (
                'promotion.md',
                datetime(2024, 2, 27, tzinfo=tz),
                one_week,
                9
            ),
            (
                'gitlab_backups.md',
                datetime(2024, 2, 26, tzinfo=tz),
                one_week * 2,
                9
            ),
            (
                'opensearch_updates.md',
                datetime(2024, 2, 26, tzinfo=tz),
                one_week * 2,
                9
            ),
            (
                'fedramp_inventory_review.md',
                datetime(2024, 3, 1, tzinfo=tz),
                one_week * 4,
                9
            ),
            (
                'audited_events_rule_set_review.md',
                datetime(2024, 4, 3, tzinfo=tz),
                364,  # Every year, (364 days)
                10
            ),
        ]
    ]
    templates = list(filter(None, templates))  # Purge what isn't scheduled

    if templates:
        assert None not in templates, templates
        main(templates, bool(args.dry_run))
    else:
        log.info('All templates are outside of the configured schedule window')
