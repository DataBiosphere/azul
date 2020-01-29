import os
import sys

import git

from azul import config
import azul.deployment

"""
Ensure that the currently checked out branch matches the selected deployment
"""


def check_branch(branch, stage):
    """
    >>> from unittest.mock import patch

    >>> with patch.object(azul.deployment, 'aws') as aws:
    ...     aws.account = '123'
    ...     check_branch('develop', 'dev')
    Traceback (most recent call last):
    ...
    RuntimeError: Protected branch 'develop' should be deployed to AWS account '122796619775', not '123'

    >>> with patch.object(azul.deployment, 'aws') as aws:
    ...     aws.account = '122796619775'
    ...     check_branch('develop', 'dev')

    >>> check_branch('issues/foo', 'prod')
    Traceback (most recent call last):
    ...
    RuntimeError: Non-protected branch 'issues/foo' can't be deployed to main deployment 'prod'

    >>> check_branch('hca/staging', 'hannes.local')
    Traceback (most recent call last):
    ...
    RuntimeError: Protected branch 'hca/staging' should be deployed to 'staging', not 'hannes.local'

    >>> check_branch('hca/staging', 'hca/integration')
    Traceback (most recent call last):
    ...
    RuntimeError: Protected branch 'hca/staging' should be deployed to 'staging', not 'hca/integration'
    """
    stage_by_branch = config.main_deployments_by_branch
    account = azul.deployment.aws.account
    try:
        expected_account, expected_stage = stage_by_branch[branch]
    except KeyError:
        if stage in [s for _, s in stage_by_branch.values()]:
            raise RuntimeError(f"Non-protected branch '{branch}' can't be deployed to main deployment '{stage}'")
    else:
        if stage != expected_stage:
            raise RuntimeError(f"Protected branch '{branch}' should be deployed to '{expected_stage}', not '{stage}'")
        elif account != expected_account:
            raise RuntimeError(
                f"Protected branch '{branch}' should be deployed to AWS account '{expected_account}', not '{account}'"
            )


def expected_stage(branch):
    return config.main_deployments_by_branch.get(branch)


def current_branch():
    try:
        # Gitlab checks out a specific commit which results in a detached HEAD
        # (no active branch). Extract the branch name from the runner environment.
        branch = os.environ['CI_COMMIT_REF_NAME']
    except KeyError:
        repo = git.Repo(config.project_root)
        branch = repo.active_branch.name
    return branch


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
