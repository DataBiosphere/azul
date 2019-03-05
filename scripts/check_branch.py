import os

import git

from azul import config


def check_branch(branch, stage):
    """
    >>> check_branch('dev', 'develop')

    >>> check_branch('feature/foo', 'prod')
    Traceback (most recent call last):
    ...
    RuntimeError: Non-protected branch 'feature/foo' can't be deployed to main deployment 'prod'

    >>> check_branch('staging', 'hannes')
    Traceback (most recent call last):
    ...
    RuntimeError: Protected branch 'staging' should be deployed to 'staging', not 'hannes'

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


if __name__ == "__main__":
    try:
        # Gitlab checks out a specific commit which results in a detached HEAD
        # (no active branch). Extract the branch name from the runner environment.
        branch = os.environ['CI_COMMIT_REF_NAME']
    except KeyError:
        repo = git.Repo(config.project_root)
        branch = repo.active_branch.name
    stage = config.deployment_stage
    check_branch(branch, stage)
