import logging
import os
import sys
from typing import (
    Sequence,
)

import git

from azul import (
    Deployment,
    config,
)
from azul.logging import (
    configure_script_logging,
)

"""
Ensure that the currently checked out branch matches the selected deployment
"""

log = logging.getLogger(__name__)


def default_deployment(branch: str | None) -> str | None:
    deployments = config.shared_deployments_for_branch(branch)
    return None if deployments is None else deployments[0].name


class BranchDeploymentMismatch(Exception):

    def __init__(self,
                 branch: str | None,
                 deployment: Deployment,
                 allowed: Sequence[Deployment] | None
                 ) -> None:
        branch = 'Detached head' if branch is None else f'Branch {branch!r}'
        if allowed is None:
            allowed = ''
        else:
            allowed = f'one of {set(d.name for d in allowed)!r} or '
        super().__init__(f'{branch} cannot be deployed to {deployment.name!r}, '
                         f'only {allowed}personal deployments.')


def check_branch(branch: str | None, deployment: str) -> None:
    deployment = Deployment(deployment)
    if deployment.is_shared:
        deployments = config.shared_deployments_for_branch(branch)
        if deployments is None or deployment not in deployments:
            raise BranchDeploymentMismatch(branch, deployment, deployments)


def target_branch() -> str | None:
    """
    In a local clone, this method returns the name of the branch currently
    checked out or ``None``, if no branch is checked out (detached HEAD). On
    GitHub and GitLab this returns the name of either the branch currently being
    built or, if the build is for a feature branch involving a pull request, the
    base branch of that feature branch.
    """
    # The comments on the environment variable names below are taken from
    #
    # https://docs.github.com/en/actions/writing-workflows/choosing-what-your-workflow-does/store-information-in-variables
    #
    # and
    #
    # https://docs.gitlab.com/ee/ci/variables/predefined_variables.html
    #
    for variable in [
        # The name of the base ref or target branch of the pull request in a
        # workflow run. This is only set when the event that triggers a workflow
        # run is either pull_request or pull_request_target. For example, main.
        #
        'GITHUB_BASE_REF',

        # The short ref name of the branch or tag that triggered the workflow
        # run. This value matches the branch or tag name shown on GitHub. For
        # example, feature-branch-1. For pull requests, the format is
        # <pr_number>/merge.
        #
        'GITHUB_REF_NAME',

        # The branch or tag name for which project is built.
        #
        'CI_COMMIT_REF_NAME',
    ]:
        try:
            branch = os.environ[variable]
        except KeyError:
            pass
        else:
            if branch:
                log.info('Target branch is %r as defined in %r', branch, variable)
                return branch
    repo = git.Repo(config.project_root)
    if repo.head.is_detached:
        branch = None
        log.info('Target branch is %r because HEAD is detached', branch)
    else:
        branch = repo.head.reference.name
        log.info('Target branch is %r because it is checked out', branch)
    return branch


def main(argv):
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--print', '-P',
                        default=False,
                        action='store_true',
                        help='Print the deployment matching the current branch or exit '
                             'with non-zero status code if no such deployment exists.')
    args = parser.parse_args(argv)
    branch = target_branch()
    if args.print:
        deployment = default_deployment(branch)
        if deployment is None:
            sys.exit(1)
        else:
            print(deployment)
    else:
        check_branch(branch, config.deployment_stage)


if __name__ == '__main__':
    configure_script_logging(log)
    main(sys.argv[1:])
