import os
import sys
from typing import Optional

import git

from azul import config

"""
Ensure that the currently checked out branch matches the selected deployment
"""


def check_branch(branch: Optional[str], stage: str) -> None:
    """
    >>> check_branch('dev', 'develop')

    >>> check_branch('feature/foo', 'prod')
    Traceback (most recent call last):
    ...
    RuntimeError: Non-protected branch 'feature/foo' can't be deployed to main deployment 'prod'

    >>> check_branch('staging', 'hannes.local')

    >>> check_branch('develop', 'hannes.local')

    >>> check_branch('staging', 'integration')
    Traceback (most recent call last):
    ...
    RuntimeError: Protected branch 'staging' should be deployed to 'staging', not 'integration'

    >>> check_branch(None, 'dev')
    Traceback (most recent call last):
    ...
    RuntimeError: Can't deploy to main deployment 'dev' from a detached head.'
    """
    stage_by_branch = config.main_deployments_by_branch
    try:
        expected_stage = stage_by_branch[branch]
    except KeyError:
        if stage in stage_by_branch.values():
            raise RuntimeError(
                f"Can't deploy to main deployment '{stage}' from a detached head.'"
                if branch is None else
                f"Non-protected branch '{branch}' can't be deployed to main deployment '{stage}'"
            )
    else:
        assert branch is not None
        if stage != expected_stage and config.is_main_deployment(stage):
            raise RuntimeError(f"Protected branch '{branch}' should be deployed to '{expected_stage}', not '{stage}'")


def expected_stage(branch: Optional[str]) -> Optional[str]:
    return config.main_deployments_by_branch.get(branch)


def current_branch() -> Optional[str]:
    try:
        # Gitlab checks out a specific commit which results in a detached HEAD
        # (no active branch). Extract the branch name from the runner environment.
        return os.environ['CI_COMMIT_REF_NAME']
    except KeyError:
        # Detached head may also occur outside of Gitlab, in which case it is
        # only allowed for personal deployments.
        repo = git.Repo(config.project_root)
        return None if repo.head.is_detached else repo.active_branch.name


def main(argv):
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--print', '-P',
                        default=False,
                        action='store_true',
                        help="Print the deployment matching the current branch or exit "
                             "with non-zero status code if no such deployment exists.")
    parser.add_argument('--personal',
                        default=False,
                        action='store_true',
                        help="Exit with non-zero status code if current deployment is a "
                             "main deployment.")
    args = parser.parse_args(argv)
    branch = current_branch()
    if args.print:
        stage = expected_stage(branch)
        if stage is None:
            sys.exit(1)
        else:
            print(stage)
    else:
        stage = config.deployment_stage
        check_branch(branch, stage)
    if args.personal:
        if config.deployment_stage in config.main_deployments_by_branch.values():
            raise RuntimeError(f"Selected deployment '{stage}' is not a personal deployment.")


if __name__ == "__main__":
    main(sys.argv[1:])
