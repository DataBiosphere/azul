import os
import sys

from github import (
    Github,
)

from azul import (
    config,
)

if __name__ == '__main__':
    gh = Github(config.github_access_token)
    repo = gh.get_repo(config.github_project)
    commit = repo.get_commit(sha=os.environ['CI_COMMIT_SHA'])
    context, status = sys.argv[1:]
    commit.create_status(state=status,
                         target_url=os.environ['CI_PIPELINE_URL'],
                         description=f'Gitlab build status is {status}',
                         context=context)
