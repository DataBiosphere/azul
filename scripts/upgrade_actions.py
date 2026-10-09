"""
Update the version pins of the GitHub actions used in this project's workflows.

Every action is pinned to a commit SHA, followed by a comment naming the
version that SHA belongs to. A SHA is the only reference that upstream cannot
repoint, which a version tag otherwise is free to be, even a fully specified
one.

For each action, the script determines the most recent release whose tag is a
fully specified version, resolves that tag to a SHA and rewrites the pin. It
leaves the modified workflows in the working copy for review; it does not
commit them. Actions pinned to a version the script can't determine are left
alone and reported.
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
    Any,
)

import attrs

from azul.lib import (
    R,
)
from azul.logging import (
    configure_script_logging,
)

log = logging.getLogger(__name__)

_project_root = Path(__file__).resolve().parent.parent
_workflow_dir = _project_root / '.github' / 'workflows'

#: Matches a step's reference to an action, capturing the owner and name of the
#: repository defining it, the path to the action within that repository, the
#: pinned reference and the comment naming the pinned version, if any
#:
_use = re.compile(
    r"""(?P<prefix>uses:\s*'?)
        (?P<repo>[\w.-]+/[\w.-]+)
        (?P<path>(?:/[\w.-]+)*)
        @(?P<ref>[\w.-]+)
        (?P<suffix>'?)
        (?:[^\S\n]*\#[^\S\n]*(?P<version>\S+))?
    """,
    re.VERBOSE)

#: Matches a tag naming a fully specified version, as opposed to one that only
#: names a major or minor version, or a pre-release
#:
_version = re.compile(r'v\d+\.\d+\.\d+')


@attrs.frozen(kw_only=True)
class Action:
    repo: str
    version: str
    sha: str

    @property
    def url(self) -> str:
        return f'https://github.com/{self.repo}'

    def compare_url(self, other: str) -> str:
        return f'{self.url}/compare/{other}...{self.version}'


def main(argv):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.parse_args(argv)
    workflows = sorted(_workflow_dir.glob('*.yml'))
    assert workflows, R('No workflows found', str(_workflow_dir))
    actions: dict[str, Action | None] = {}
    for workflow in workflows:
        content = workflow.read_text()
        updated = _update(content, actions)
        if updated == content:
            log.info('%s is up to date', _relative(workflow))
        else:
            workflow.write_text(updated)
            log.info('%s was updated, review it with `git diff`',
                     _relative(workflow))


def _relative(path: Path) -> str:
    return str(path.relative_to(_project_root))


def _update(content: str, actions: dict[str, Action | None]) -> str:
    def replace(match: re.Match) -> str:
        repo = match.group('repo')
        if repo not in actions:
            actions[repo] = _latest_action(repo)
        action = actions[repo]
        # The comment names the pinned version more precisely than the pinned
        # reference does, once that reference is a SHA
        current = match.group('version') or match.group('ref')
        if action is None:
            log.warning('%s is pinned to %s and was left alone', repo, current)
            return match.group(0)
        elif action.version == current:
            log.info('%s is current at %s', repo, current)
        else:
            log.info('%s goes from %s to %s, see %s',
                     repo, current, action.version,
                     action.compare_url(current))
        return ''.join([
            match.group('prefix'),
            repo,
            match.group('path'),
            '@', action.sha,
            match.group('suffix'),
            '  # ', action.version
        ])

    return _use.sub(replace, content)


def _latest_action(repo: str) -> Action | None:
    tags = _tags(repo)
    versions = sorted(
        (tuple(map(int, tag[1:].split('.'))), tag)
        for tag in tags
        if _version.fullmatch(tag)
    )
    if versions:
        _, version = versions[-1]
        return Action(repo=repo, version=version, sha=_sha(repo, tags[version]))
    else:
        return None


def _tags(repo: str) -> dict[str, Any]:
    refs = _gh(f'repos/{repo}/git/matching-refs/tags/v')
    return {
        ref['ref'].removeprefix('refs/tags/'): ref['object']
        for ref in refs
    }


def _sha(repo: str, object: Any) -> str:
    # An annotated tag points at a tag object, which in turn points at the
    # commit. A lightweight one points at the commit directly.
    if object['type'] == 'tag':
        object = _gh(object['url'])['object']
    sha = object['sha']
    assert re.fullmatch(r'[0-9a-f]{40}', sha), R('Not a commit SHA', repo, sha)
    return sha


def _gh(endpoint: str) -> Any:
    result = subprocess.run(['gh', 'api', endpoint],
                            capture_output=True, text=True, check=True)
    return json.loads(result.stdout)


if __name__ == '__main__':
    configure_script_logging(log)
    main(sys.argv[1:])
