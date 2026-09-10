"""
Fail if the current branch modifies `pyproject.toml` but leaves the transitive
dependencies stale, i.e. if `make requirements_update` would still have an
effect. Branches that leave that file alone are exempt, so that a new release of
a transitive dependency can't fail them.
"""

import logging
import subprocess
import sys

from azul.logging import (
    configure_script_logging,
)

log = logging.getLogger(__name__)

#: The message `uv` emits when an upgrade would not change the lock file. We
#: require its presence instead of rejecting the messages that report changes,
#: so that a reworded message fails the build instead of silently passing it.
#:
no_changes = 'No lockfile changes detected'


def run(*args: str, capture: bool = True) -> subprocess.CompletedProcess:
    """
    Run the given command, optionally capturing its output, with stderr folded
    into stdout. Uncaptured output is inherited, so that progress reported by a
    long-running command appears in the build log while it runs.
    """
    log.info('Running %r', args)
    return subprocess.run(args,
                          text=True,
                          stdout=subprocess.PIPE if capture else None,
                          stderr=subprocess.STDOUT if capture else None)


def develop_remote() -> str | None:
    """
    The remote to fetch `develop` from, or `None` if there isn't one. On GitLab
    the remote is `origin`, but a local clone names its remotes after the
    instance they refer to, so we fall back to whichever one `develop` tracks.
    """
    if run('git', 'remote', 'get-url', 'origin').returncode == 0:
        remote = 'origin'
    else:
        process = run('git', 'config', '--get', 'branch.develop.remote')
        remote = process.stdout.strip() if process.returncode == 0 else None
    return remote


def fetch_develop(remote: str) -> int:
    """
    Fetch the tip of `develop`, and only that. Because `--depth` counts along
    every parent of a merge, deepening a history as merge-heavy as ours is
    disproportionately expensive: `--depth 100` pulls thousands of commits and
    several hundred MB. The tip is all a branch rebased onto it needs, and any
    other branch is asked to rebase rather than made to pay for it.

    A shallow fetch would truncate a complete clone, so it is only used on one
    that is already shallow. `--progress` reports how much was transferred,
    which is otherwise invisible in a build log, because git only reports it on
    a terminal. It rules out `--quiet`, which suppresses that report even when
    both are given.
    """
    process = run('git', 'rev-parse', '--is-shallow-repository')
    depth = ['--depth', '1'] if process.stdout.strip() == 'true' else []
    return run('git', 'fetch', '--progress', '--no-recurse-submodules',
               *depth, remote, 'develop', capture=False).returncode


def diverged_at() -> str | None:
    """
    The commit at which this branch diverged from `develop`, or `None` if that
    can't be determined from the history at hand. GitLab builds the tip of the
    pushed branch, not a merge commit, so this is what the branch's own
    modifications have to be measured against — not `HEAD^`, which is merely the
    previous commit on the branch, and not the tip of `develop`, which may have
    moved on since.

    Note that `git merge-base` reports a history too shallow to contain the
    point of divergence the same way it reports genuinely unrelated histories,
    by exiting non-zero and saying nothing.

    How much history is at hand is what `GIT_DEPTH` governs in `.gitlab-ci.yml`.
    It is a GitLab concept, not one that `git` itself knows about; each instance
    otherwise configures its own depth, and the shallowest of them is too
    shallow for this. The depth set there covers all but three of the last
    hundred branches merged into `develop`, and those three are asked to rebase,
    which puts the tip of `develop` back within reach.
    """
    process = run('git', 'merge-base', 'FETCH_HEAD', 'HEAD')
    return process.stdout.strip() if process.returncode == 0 else None


def modifies_requirements(base: str) -> bool:
    """
    Whether this branch modifies `pyproject.toml` relative to the given commit.
    """
    return run('git', 'diff', '--quiet', base, 'HEAD',
               '--', 'pyproject.toml').returncode != 0


def check_freshness() -> int:
    """
    Report what an upgrade would do, instead of performing one, so that the
    working copy is left clean and this doesn't depend on a dirty one being
    detected downstream. `uv` exits zero whether or not upgrades exist, so the
    verdict has to be taken from its output.
    """
    process = run('uv', 'lock', '--upgrade', '--dry-run')
    if process.returncode != 0:
        log.error('%s', process.stdout.strip())
        log.error('Unable to determine whether the transitive dependencies are stale')
        status = 1
    else:
        log.info('%s', process.stdout.strip())
        if no_changes in process.stdout:
            log.info('This branch modifies pyproject.toml, its dependencies are up to date')
            status = 0
        else:
            log.error('This branch modifies pyproject.toml but its dependencies are stale')
            log.error("Run 'make requirements_update' and commit the updated uv.lock")
            status = 1
    return status


def main() -> None:
    remote = develop_remote()
    if remote is None:
        log.error("Found neither a remote called 'origin' nor one tracked by 'develop'")
        status = 1
    elif fetch_develop(remote) != 0:
        log.error('Failed to fetch develop from %r', remote)
        status = 1
    else:
        base = diverged_at()
        if base is None:
            log.error('Unable to determine where this branch diverged from develop')
            log.error('It, or develop since, is longer than the configured fetch depth')
            log.error('Rebase this branch onto develop, or increase the clone '
                      'depth, which on GitLab can be configured in the project '
                      'settings, or per job via the GIT_DEPTH variable.')
            status = 1
        elif modifies_requirements(base):
            status = check_freshness()
        else:
            log.info("This branch doesn't modify pyproject.toml, nothing to check")
            status = 0
    sys.exit(status)


if __name__ == '__main__':
    configure_script_logging(log)
    main()
