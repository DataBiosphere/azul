from argparse import (
    ArgumentParser,
)
from dataclasses import (
    dataclass,
    field,
)
from datetime import (
    date,
    datetime,
)
import json
import logging
from operator import (
    itemgetter,
)
from pathlib import (
    Path,
)
import subprocess
import sys
from typing import (
    TypedDict,
)
import zoneinfo

from azul import (
    require,
)

tz = zoneinfo.ZoneInfo('America/Los_Angeles')

log = logging.getLogger('azul.github.schedule')


class FrontMatter(TypedDict):
    name: str
    about: str
    title: str
    labels: str
    assignees: str
    _repository: str
    _start: str
    _period: str


@dataclass(kw_only=True)
class IssueTemplate:
    path: Path
    dry_run: bool
    properties: FrontMatter = field(init=False)
    body: str = field(init=False)

    def __post_init__(self):
        """
        Load the issue template at the given path and parse any YAML front
        matter embedded in the template. GitHub uses the front-matter in issue
        templates to allow for customizing issue properties other than the
        issue's description, such as its title.

        https://jekyllrb.com/docs/front-matter/
        """
        with open(self.path) as f:
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
            self.body = f.read()
            self.properties = front_matter

    def is_eligible(self, now: datetime) -> bool | None:
        try:
            start = self.properties['_start']
        except KeyError:
            return None
        else:
            period = self.properties['_period']
            return self._is_eligible(start, period, now) or self.dry_run

    @classmethod
    def _is_eligible(cls, start: str, period: str, now: datetime):
        """
        >>> f = IssueTemplate._is_eligible
        >>> f('2024-04-04T10:00','1 year',  datetime(2024, 4, 4, 10, 0, tzinfo=tz))
        True
        >>> f('2024-04-04T10:00','2 years', datetime(2025, 4, 4, 10, 0, tzinfo=tz))
        False
        >>> f('2024-04-04T10:00','2 years', datetime(2026, 4, 4, 10, 0, tzinfo=tz))
        True

        >>> f('2024-03-01T09:00','1 month',  datetime(2024, 4, 1, 9, 51, tzinfo=tz))
        True
        >>> f('2024-03-01T09:00','2 months', datetime(2024, 4, 1, 9, 51, tzinfo=tz))
        False
        >>> f('2024-03-01T09:00','2 months', datetime(2024, 5, 1, 9, 51, tzinfo=tz))
        True

        >>> f('2023-11-27T09:00','14 days', datetime(2024, 3, 11, 9, 00, tzinfo=tz))
        False
        >>> f('2023-11-27T09:00','14 days', datetime(2024, 3, 18, 9, 00, tzinfo=tz))
        True

        >>> f('2024-03-01T09:00:00Z','1 hour', datetime.now())
        Traceback (most recent call last):
        ...
        AssertionError: Start time must not specify a timezone

        >>> f('2024-03-01T09:01','1 month', datetime.now())
        ... # doctest: +NORMALIZE_WHITESPACE
        Traceback (most recent call last):
        ...
        AssertionError: ('Start time must be on the hour',
        datetime.datetime(2024, 3, 1, 9, 1, tzinfo=zoneinfo.ZoneInfo(key='America/Los_Angeles')))

        >>> f('2024-03-01T09:00','1 hour', datetime.now())
        Traceback (most recent call last):
        ...
        AssertionError: ('Invalid time unit in period', 'hour')
        """
        start = datetime.fromisoformat(start)
        assert start.tzinfo is None, 'Start time must not specify a timezone'
        start = start.replace(tzinfo=tz)
        assert (start.minute, start.second, start.microsecond) == (0, 0, 0), (
            'Start time must be on the hour',
            start
        )
        period, _, unit = period.partition(' ')
        period = int(period)
        unit = unit.removesuffix('s')
        match unit:
            case 'year':
                actual = (now.year - start.year) % period, now.month, now.day, now.hour
                expected = 0, start.month, start.day, start.hour
            case 'month':
                actual = (now.month - start.month) % period, now.day, now.hour
                expected = 0, start.day, now.hour
            case 'day':
                hour, start = start.hour, start.replace(hour=0)
                actual = (now - start).days % period, now.hour
                expected = 0, hour
            case _:
                assert False, ('Invalid time unit in period', unit)
        return actual == expected

    def create_issue(self, title_date: date) -> None:
        title = self.properties['title'] + ' ' + str(title_date)
        flags = []

        def accumulate_command_flags(option, value):
            if option != 'repo':
                option = f'add-{option}'
            flag = f'--{option}={value}'
            if value:
                flags.append(flag)
            else:
                msg = 'The defined front-matter keys must be explicitly set'
                require(value is None, msg, f'{flag}')

        accumulate_command_flags('repo', self.properties.get('_repository'))
        command = [
            'gh', 'issue', 'list',
            *flags,
            f'--search=in:title "{title}"',
            '--json=number',
            '--limit=10',
        ]
        log.info('Running %r', command)
        process = subprocess.run(command, check=True, stdout=subprocess.PIPE)
        results = json.loads(process.stdout)
        issues = set(map(itemgetter('number'), results))

        if issues:
            log.info('At least one matching issue already exists: %r', issues)
        else:
            command = [
                'gh', 'issue', 'create',
                *flags,
                f'--title={title}',
                f'--body={self.body}'
            ]
            edit_command, flags = ['gh', 'issue', 'edit', *flags], []
            accumulate_command_flags('assignee', self.properties.get('assignees'))
            accumulate_command_flags('label', self.properties.get('labels'))

            def get_issue_number() -> str:
                if self.dry_run:
                    log.info('Would run %r', command)
                    issue = '0123'
                else:
                    log.info('Running %r', command)
                    issue = subprocess.run(command, check=True, stdout=subprocess.PIPE).stdout
                    issue = issue.decode().strip()
                    print(issue)
                    issue = issue.rsplit('/', 1)[1]
                return issue

            issue_number = get_issue_number()
            for command in [edit_command + [issue_number, flag] for flag in flags]:
                if self.dry_run:
                    log.info('Would run %r', command)
                else:
                    log.info('Running %r', command)
                    subprocess.run(command, check=True, stderr=subprocess.PIPE, text=True)


def main(args):
    parser = ArgumentParser(description='Create GitHub issues from templates, '
                                        'at a schedule defined in the front '
                                        'matter of each template.')
    parser.add_argument('--dry-run',
                        action='store_true',
                        help='Do not modify anything, just log what would be modified. '
                             'Assume an issue is due for every template.')
    options = parser.parse_args(args)
    now = datetime.now(tz)
    for template in Path('.github/ISSUE_TEMPLATE').glob('*.md'):
        self = IssueTemplate(path=template, dry_run=options.dry_run)
        match self.is_eligible(now):
            case None:
                log.info('Ignoring issue template %s since it defines no schedule.', self.path)
            case False:
                log.info('Issue template %s is ineligible', self.path)
            case True:
                self.create_issue(now.date())


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s %(levelname)+7s %(name)s: %(message)s')
    main(sys.argv[1:])
