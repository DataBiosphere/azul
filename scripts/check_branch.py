import os
import sys

import git

from azul import config

"""
Ensure that the currently checked out branch matches the selected deployment
"""


def check_branch(branch, stage):
    """
    >>> check_branch('dev', 'develop')

    >>> check_branch('feature/foo', 'prod')
    Traceback (most recent call last):
    ...
    RuntimeError: Non-protected branch 'feature/foo' can't be deployed to main deployment 'prod'

    >>> check_branch('staging', 'hannes.local')
    Traceback (most recent call last):
    ...
    RuntimeError: Protected branch 'staging' should be deployed to 'staging', not 'hannes.local'

    >>> check_branch('staging', 'integration')
    Traceback (most recent call last):
    ...
    RuntimeError: Protected branch 'staging' should be deployed to 'staging', not 'integration'
    """
    stage_by_branch = config.main_deployments_by_branch
    try:
        expected_stage = stage_by_branch[branch]
    except KeyError:
        if stage in stage_by_branch.values():
            raise RuntimeError(f"Non-protected branch '{branch}' can't be deployed to main deployment '{stage}'")
    else:
        if stage != expected_stage:
            raise RuntimeError(f"Protected branch '{branch}' should be deployed to '{expected_stage}', not '{stage}'")


def expected_stage(branch):
    return config.main_deployments_by_branch.get(branch)


def current_branch():
    try:
        # Gitlab checks out a specific commit which results in a detached HEAD
        # (no active branch). Extract the branch name from the runner environment.
        branch_name = os.environ['CI_COMMIT_REF_NAME']
    except KeyError:
        # Detached head may also occur outside of the gitlab environment, in
        # which case it is only allowed for personal deployments.
        repo = git.Repo(config.project_root)
        branch_name = 'DETACHED HEAD' if repo.head.is_detached else repo.active_branch.name
    return branch_name


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
