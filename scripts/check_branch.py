import os
import sys
from typing import (
    Optional,
    Sequence,
)

import git

from azul import (
    config,
)

"""
Ensure that the currently checked out branch matches the selected deployment
"""


def default_deployment(branch: Optional[str]) -> str:
    deployments = config.main_deployments_for_branch(branch)
    return None if deployments is None else deployments[0]


class BranchDeploymentMismatch(Exception):

    def __init__(self,
                 branch: Optional[str],
                 deployment: str,
                 allowed: Optional[Sequence[str]]
                 ) -> None:
        branch = 'Detached head' if branch is None else f'Branch {branch!r}'
        allowed = '' if allowed is None else f'one of {set(allowed)!r} or '
        super().__init__(f'{branch} cannot be deployed to {deployment!r}, '
                         f'only {allowed}personal deployments.')


def check_branch(branch: Optional[str], deployment: str) -> None:
    if config.is_main_deployment(deployment):
        deployments = config.main_deployments_for_branch(branch)
        if deployments is None or deployment not in deployments:
            raise BranchDeploymentMismatch(branch, deployment, deployments)


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
    args = parser.parse_args(argv)
    branch = gitlab_branch() or local_branch()
    if args.print:
        deployment = default_deployment(branch)
        if deployment is None:
            sys.exit(1)
        else:
            print(deployment)
    else:
        check_branch(branch, config.deployment_stage)


if __name__ == '__main__':
    main(sys.argv[1:])
