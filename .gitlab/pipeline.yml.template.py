"""
Render the GitLab CI/CD pipeline. The output is JSON, which GitLab accepts as
YAML. GitLab requires the files it includes to have a `.yml` extension.
"""
from azul.template import (
    emit,
)


def schedule_rule(schedule: str) -> str:
    return (
        "$CI_PIPELINE_SOURCE == 'schedule'"
        f" && $azul_gitlab_schedule == '{schedule}'"
    )


emit({
    'workflow': {
        'name': '$azul_gitlab_pipeline_name',
        'rules': [
            {
                'if': "$CI_PIPELINE_SOURCE == 'push' && $CI_COMMIT_BRANCH",
                'variables': {
                    'azul_gitlab_pipeline_name': '$CI_COMMIT_TITLE'
                }
            },
            {
                'if': "$CI_PIPELINE_SOURCE == 'schedule'",
                'variables': {
                    'azul_gitlab_pipeline_name': 'Scheduled $azul_gitlab_schedule'
                }
            }
        ]
    },
    'variables': {
        'azul_image': '$CI_REGISTRY_IMAGE',
        'azul_image_tag': '$CI_PIPELINE_ID'
    },
    'stages': [
        'build_image',
        'setup',
        'test',
        'deploy',
        'early_reindex',
        'integration_test',
        'teardown',
        'reindex',
        'schedule'
    ],
    '.on_push': {
        'rules': [
            {
                'if': "$CI_PIPELINE_SOURCE == 'push' && $CI_COMMIT_BRANCH"
            }
        ]
    },
    # Build an image containing the build prerequisites (python, terraform,
    # etc). This lets us exploit Docker's layer caching to speed up the build.
    # The image will only be rebuilt after changes to the Dockerfile,
    # pyproject.toml, uv.lock, common.mk and the Makefile.
    #
    'build_image': {
        'extends': '.on_push',
        'stage': 'build_image',
        'timeout': '6m',
        'rules': [
            {
                'if': '$CI_COMMIT_BRANCH'
            }
        ],
        'script': [
            'printenv CI_REGISTRY_PASSWORD'
            ' | docker login --username $CI_REGISTRY_USER'
            ' --password-stdin $CI_REGISTRY_IMAGE',
            'set -o allexport ; source environment.boot ; set +o allexport',
            'make docker_push'
        ]
    },
    '.base': {
        'dependencies': [
            'build_image'
        ],
        'image': '${azul_image}:${azul_image_tag}',
        'before_script': [
            # Copy files like environment.local into the build directory
            'cp -vR /etc/gitlab/azul/* .',
            # Load global defaults
            'source environment',
            'deployment=$(PYTHONPATH=src python scripts/check_branch.py --print)',
            '_set_deployment $deployment',
            '_refresh',
            'status_context="gitlab/${azul_gitlab_instance_name}/${AZUL_DEPLOYMENT_STAGE}"',
            'make clean'
        ]
    },
    '.base_on_push': {
        'extends': [
            '.on_push',
            '.base'
        ]
    },
    'setup': {
        'extends': '.base_on_push',
        'stage': 'setup',
        'timeout': '45s',
        'script': [
            'python scripts/github_status_check.py "${status_context}" pending'
        ]
    },
    'test': {
        'extends': '.base_on_push',
        'stage': 'test',
        'timeout': '1h 30m',
        'variables': {
            # Enough history for `scripts/check_transitive_requirements.py` to
            # find where this branch diverged from `develop`. Set here, rather
            # than per instance, so that the check behaves the same everywhere.
            'GIT_DEPTH': '100'
        },
        'script': [
            'make check_requirements',
            'if python -m azul -t config.deployment.is_lower_sandbox ;'
            ' then make check_transitive_requirements ; fi',
            'make format',
            'make isort',
            'make -C lambdas openapi',
            'make environment.boot',
            'make -C .github',
            'make -C .gitlab',
            'make anvil_schema',
            'make check_clean',
            'make pep8',
            'mypy',
            'AZUL_DEBUG=0 make test'
        ]
    },
    'deploy_base': {
        'extends': '.base_on_push',
        'stage': 'deploy',
        'timeout': '20m',
        'script': [
            '_set_deployment $deployment.base',
            '_refresh',
            'cd terraform/base',
            'make auto_apply'
        ],
        'artifacts': {
            'paths': [
                'terraform/base/plan.json'
            ]
        }
    },
    'deploy': {
        'extends': '.base_on_push',
        'stage': 'deploy',
        'timeout': '45m',
        'needs': [
            'build_image',
            'deploy_base'
        ],
        'script': [
            'make auto_deploy',
            'make create'
        ],
        'artifacts': {
            'paths': [
                'terraform/plan.json'
            ]
        }
    },
    'deploy_browser': {
        'extends': '.base_on_push',
        'stage': 'deploy',
        # Initial deploys can take a while
        'timeout': '20m',
        'needs': [
            'build_image',
            'deploy_base',
            'deploy'
        ],
        'script': [
            'if [[ ! -e deployments/$deployment.browser ]] ; then exit 0 ; fi',
            '_set_deployment $deployment.browser',
            '_refresh',
            'cd terraform/browser',
            'make auto_apply'
        ],
        'artifacts': {
            'paths': [
                'terraform/browser/plan.json'
            ]
        }
    },
    # FIXME: Use a dynamic GitLab job timeout based on deployment
    #        https://github.com/DataBiosphere/azul/issues/5404
    #
    'integration_test': {
        'extends': '.base_on_push',
        'stage': 'integration_test',
        'timeout': '2h',
        'script': [
            'export azul_it_flags="${azul_it_flags:+$azul_it_flags }unattended"',
            'make integration_test'
        ]
    },
    'on_success': {
        'extends': '.base_on_push',
        'stage': 'teardown',
        'when': 'on_success',
        'timeout': '30s',
        'script': [
            'python scripts/github_status_check.py "${status_context}" success'
        ]
    },
    'on_failure': {
        'extends': '.base_on_push',
        'stage': 'teardown',
        'when': 'on_failure',
        'timeout': '30s',
        'script': [
            'python scripts/github_status_check.py "${status_context}" failure'
        ]
    },
    # FIXME: Use a dynamic GitLab job timeout based on deployment
    #        https://github.com/DataBiosphere/azul/issues/5404
    #
    'early_reindex': {
        'extends': '.base_on_push',
        'stage': 'early_reindex',
        'when': 'manual',
        'timeout': '4d',
        'script': [
            'make reindex'
        ]
    },
    # FIXME: Use a dynamic GitLab job timeout based on deployment
    #        https://github.com/DataBiosphere/azul/issues/5404
    #
    'reindex': {
        'extends': '.base_on_push',
        'stage': 'reindex',
        'when': 'manual',
        'timeout': '4d',
        'script': [
            'make reindex'
        ]
    },
    'sell_unused_slots': {
        'extends': '.base',
        'stage': 'schedule',
        'timeout': '1m',
        'rules': [
            {
                'if': schedule_rule('sell_unused_slots')
            }
        ],
        'script': [
            'python scripts/sell_slots.py'
        ]
    },
    'fedramp_inventory': {
        'extends': '.base',
        'stage': 'schedule',
        'timeout': '5m',
        'rules': [
            {
                'if': schedule_rule('fedramp_inventory')
            }
        ],
        'script': [
            'python scripts/compliance/fedramp_inventory.py fedramp_inventory.xlsx'
        ],
        'artifacts': {
            'paths': [
                'fedramp_inventory.xlsx'
            ],
            'expire_in': '1 year'
        }
    },
    'github_schedule': {
        'extends': '.base',
        'stage': 'schedule',
        'timeout': '5m',
        'rules': [
            {
                'if': schedule_rule('github_schedule')
            }
        ],
        'script': [
            'export GITHUB_TOKEN=$azul_github_access_token GITHUB_REPO=$azul_github_project',
            'python .github/workflows/schedule.py'
        ]
    }
})
