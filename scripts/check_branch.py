import os
import sys
from typing import (
    Optional,
)

import git

from azul import (
    config,
)

"""
Ensure that the currently checked out branch matches the selected deployment
"""


def check_branch(branch: Optional[str], stage: str) -> None:
    """
    >>> check_branch('dev', 'develop')

    >>> check_branch('feature/foo', 'prod')
    Traceback (most recent call last):
    ...
    RuntimeError: Feature branch 'feature/foo' cannot be deployed to main deployment 'prod'

    >>> check_branch('staging', 'hannes.local')

    >>> check_branch('develop', 'hannes.local')

    >>> check_branch('staging', 'integration')
    Traceback (most recent call last):
    ...
    RuntimeError: Protected branch 'staging' should be deployed to one of ['staging'], not 'integration'

    >>> check_branch('prod', 'prod2')

    >>> check_branch(None, 'dev')
    Traceback (most recent call last):
    ...
    RuntimeError: Cannot deploy to main deployment 'dev' from a detached head.

    """
    try:
        expected_stages = config.main_deployments_by_branch[branch]
    except KeyError:
        if stage in config.main_branches_by_deployment:
            raise RuntimeError(
                f'Cannot deploy to main deployment {stage!r} from a detached head.'
                if branch is None else
                f'Feature branch {branch!r} cannot be deployed to main deployment {stage!r}'
            )
    else:
        assert branch is not None
        if stage not in expected_stages and config.is_main_deployment(stage):
            raise RuntimeError(f'Protected branch {branch!r} should be deployed '
                               f'to one of {list(expected_stages)!r}, not {stage!r}')


def gitlab_branch() -> Optional[str]:
    """
    Return the current branch if we're on GitLab, else `None`
    """
    # Gitlab checks out a specific commit which results in a detached HEAD
    # (no active branch). Extract the branch name from the runner environment.
    return os.environ.get('CI_COMMIT_REF_NAME')


def local_branch() -> Optional[str]:
    """
    Return `None` if detached head, else the current branch
    """
    repo = git.Repo(config.project_root)
    return None if repo.head.is_detached else repo.active_branch.name


def main(argv):
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--print', '-P',
                        default=False,
                        action='store_true',
                        help='Print the deployment matching the current branch or exit '
                             'with non-zero status code if no such deployment exists.')
    parser.add_argument('--personal',
                        default=False,
                        action='store_true',
                        help='Exit with non-zero status code if current deployment is a '
                             'main deployment.')
    args = parser.parse_args(argv)
    if args.print:
        branch = gitlab_branch() or local_branch()
        stages = config.main_deployments_by_branch[branch]
        if stages:
            print(stages[0])  # the first stage is the default one
        else:
            sys.exit(1)
    else:
        stages = config.deployment_stage
        branch = gitlab_branch()
        if branch is None:
            if stages == 'sandbox':
                raise RuntimeError(f'Only the GitLab runner should deploy to {stages!r}')
            branch = local_branch()
        check_branch(branch, stages)
    if args.personal:
        if config.deployment_stage in config.main_branches_by_deployment.values():
            raise RuntimeError(f'Selected deployment {stages!r} is not a personal deployment.')


if __name__ == '__main__':
    main(sys.argv[1:])
