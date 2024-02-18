from enum import (
    Enum,
)
from itertools import (
    chain,
)
from os.path import (
    basename,
    dirname,
)
import sys
import textwrap
from typing import (
    AbstractSet,
    Mapping,
)

from more_itertools import (
    flatten,
    one,
    stagger,
)

from azul import (
    iif,
)
from azul.collections import (
    OrderedSet,
)
from azul.strings import (
    join_grammatically,
)
from azul.template import (
    emit_text,
)
from azul.types import (
    JSONs,
)


def emit_checklist(checklist: JSONs):
    def comment(i, _):
        return '<!--', *wrap(i), '-->'

    def p(i, _):
        return '', *wrap(i)

    def h1(i, _):
        return '', '', '## ' + i['content']

    def h2(i, _):
        return '', '', '### ' + i['content']

    def cli(i, j):
        return (
            *margin(i, j),
            '- [ ] ' + i['content']
            + (' <sub>' + i['alt'] + '</sub>' if i.get('alt') is not None else '')
        )

    def li(i, j):
        return *margin(i, j), '- ' + i['content']

    handlers = locals().copy()
    del handlers['checklist']

    def margin(i, j):
        return [] if (j and j['type'] == i['type']) else ['']

    def wrap(i): return textwrap.wrap(i['content'], 80)

    with emit_text() as f:
        f.writelines(line + '\n' for line in flatten(
            handlers[i['type']](i, j)
            for j, i in stagger(filter(bool, checklist), offsets=(-1, 0))
        ))


dir = 'PULL_REQUEST_TEMPLATE'

images_we_build_ourselves = {
    k: f'https://hub.docker.com/repository/docker/ucscgi/azul-{k.lower()}'
    for k in ['Elasticsearch', 'PyCharm']
}

prod = 'prod'
develop = 'develop'


class T(Enum):
    default = 'pull_request_template.md'
    promotion = dir + '/promotion.md'
    hotfix = dir + '/hotfix.md'
    backport = dir + '/backport.md'
    upgrade = dir + '/upgrade.md'

    @property
    def file(self):
        return basename(self.value)

    @property
    def dir(self):
        return dirname(self.value)

    @property
    def target_branch(self):
        return prod if self in (T.promotion, T.hotfix) else develop

    @property
    def issues(self):
        default = self is T.default

        class S(str):

            def __call__(self, then, otherwise):
                return then if default else otherwise

        return S('issue' + iif(default, 's'))

    @property
    def target_deployments(self) -> Mapping[str, str]:
        """
        Maps the name of each deployment to that of the respective sandbox.
        """
        return (
            {
                # There currently is no sandbox for production deployments
                prod: None
            }
            if self.target_branch == prod else
            {
                'dev': 'sandbox',
                'anvildev': 'anvilbox',
                'anvilprod': 'hammerbox'
            }
        )

    @property
    def downstream_deployments(self) -> AbstractSet[str]:
        return OrderedSet(chain(
            self.target_deployments.keys(),
            self.promotion.target_deployments if self.target_branch == develop else []
        ))

    @property
    def labels_to_promote(self) -> tuple[str, ...]:
        return (
            'deploy:shared',
            'deploy:gitlab',
            'deploy:runner',
            'reindex:partial',
            *('reindex:' + d for d in self.target_deployments)
        )

    @property
    def deploy_shared_target(self) -> str:
        return 'apply_keep_unused' if self.target_branch == develop else 'apply'


def bq(s):
    return '`' + s + '`'


def main():
    t = one(tt for tt in T if tt.value == sys.argv[1])
    emit_checklist(
        [
            {
                'type': 'comment',
                'content': {
                    T.default: f'This is the PR template for regular PRs against {bq(develop)}. '
                               "Edit the URL in your browser's location bar, appending either "
                               + join_grammatically([f'`&template={tt.file}`' for tt in T if tt.dir],
                                                    joiner=', ',
                                                    last_joiner=' or ')
                               + ' to switch the template.',
                    T.backport: f'This is the PR template for backport PRs against {bq(develop)}.',
                    T.upgrade: 'This is the PR template for upgrading Azul dependencies.',
                    T.hotfix: f'This is the PR template for hotfix PRs against {bq(prod)}.',
                    T.promotion: f'This is the PR template for promotion PRs against {bq(prod)}.'
                }[t]
            },
            iif(t is not T.backport, {
                'type': 'p',
                'content': f'Connected {t.issues}: #0000'

            }),
            {
                'type': 'h1',
                'content': 'Checklist'
            },
            {
                'type': 'h2',
                'content': 'Author'
            },
            iif(t is T.default, {
                'type': 'cli',
                'content': 'PR is a draft'
            }),
            {
                'type': 'cli',
                'content': f'Target branch is `{t.target_branch}`'
            },
            {
                'type': 'cli',
                'content': 'Name of PR branch matches `' + {
                    T.default: 'issues/<GitHub handle of author>/<issue#>-<slug>',
                    T.promotion: 'promotions/yyyy-mm-dd',
                    T.hotfix: 'hotfixes/<GitHub handle of author>/<issue#>-<slug>',
                    T.upgrade: 'upgrades/yyyy-mm-dd',
                    T.backport: 'backports/<7-digit SHA1 of most recent backported commit>'
                }[t] + '`'
            },
            iif(t is not t.backport, {
                'type': 'cli',
                'content': {
                    T.default: 'On ZenHub, PR is connected to all issues it (partially) resolves',
                    T.upgrade: 'On ZenHub, PR is connected to the upgrade issue it resolves',
                    T.hotfix: 'On ZenHub, PR is connected to the issue it hotfixes',
                    T.promotion: 'On ZenHub, PR is connected to the promotion issue it resolves',
                    T.backport: None
                }[t]
            }),
            iif(t not in (T.backport, T.upgrade), {
                'type': 'cli',
                'content': f'PR description links to connected {t.issues}'
            }),
            iif(t is T.promotion, {
                'type': 'cli',
                'content': 'Title of connected issue matches `Promotion yyyy-mm-dd`'
            }),
            {
                'type': 'cli',
                'content': {
                    t.default: 'PR title matches<sup>1</sup> that of a connected issue',
                    t.promotion: 'PR title starts with title of connected issue',
                    t.hotfix: 'PR title is `Hotfix: ` followed by title of connected issue',
                    t.upgrade: 'PR title matches `Upgrade dependencies yyyy-mm-dd`',
                    t.backport: 'PR title contains the 7-digit SHA1 of the backported commits'
                }[t],
                'alt': iif(t is t.default, "or comment in PR explains why they're different", None)
            },
            iif(t is not T.backport, {
                'type': 'cli',
                'content': f"PR title references {t.issues('all', 'the')} connected {t.issues}"
            }),
            *(
                [
                    {
                        'type': 'cli',
                        'content': 'PR title references the issues relating to the backported commits'
                    },
                    {
                        'type': 'cli',
                        'content': 'PR title references the PRs that introduced the backported commits'
                    }
                ]
                if t is T.backport else
                []
            ),
            iif(t is T.default, {
                'type': 'cli',
                'content': 'For each connected issue, there is at least one commit whose title references that issue'
            }),
            *iif(t is T.default, [
                {
                    'type': 'h2',
                    'content': 'Author (partiality)'
                },
                {
                    'type': 'cli',
                    'content': 'Added `p` tag to titles of partial commits'
                },
                {
                    'type': 'cli',
                    'content': 'This PR is labeled `partial`',
                    'alt': 'or completely resolves all connected issues'
                },
                {
                    'type': 'cli',
                    'content': 'This PR partially resolves each of the connected issues',
                    'alt': 'or does not have the `partial` label'
                }
            ]),
            iif(t is T.default, {
                'type': 'p',
                'content': '<sup>1</sup> when the issue title describes a problem, the corresponding PR title is '
                           '`Fix: ` followed by the issue title'
            }),
            *iif(t is T.default, [
                {
                    'type': 'h2',
                    'content': 'Author (chains)'
                },
                {
                    'type': 'cli',
                    'content': 'This PR is blocked by previous PR in the chain',
                    'alt': 'or is not chained to another PR'
                },
                {
                    'type': 'cli',
                    'content': 'The blocking PR is labeled `base`',
                    'alt': 'or this PR is not chained to another PR'
                },
                {
                    'type': 'cli',
                    'content': 'This PR is labeled `chained`',
                    'alt': 'or is not chained to another PR'
                }
            ]),
            *iif(t in (T.default, T.promotion), [
                {
                    'type': 'h2',
                    'content': 'Author (reindex, API changes)'
                },
                iif(t is T.default, {
                    'type': 'cli',
                    'content': 'Added `r` tag to commit title',
                    'alt': 'or this PR does not require reindexing'
                }),
                *[
                    {
                        'type': 'cli',
                        'content': f'This PR is labeled `reindex:{d}`',
                        'alt': f'or does not require reindexing `{d}`'
                    }
                    for d in t.target_deployments
                ],
                {
                    'type': 'cli',
                    'content': 'This PR is labeled `reindex:partial` and '
                               + 'its description documents the specific reindexing procedure for '
                               + join_grammatically([f'`{d}`' for d in t.downstream_deployments]),
                    'alt': 'or requires a full reindex '
                           + iif(len(t.downstream_deployments) == 1,
                                 'or is not labeled',
                                 'or carries none of the labels ')
                           + join_grammatically([f'`reindex:{d}`' for d in t.downstream_deployments])
                },
                {
                    'type': 'cli',
                    'content': 'This PR and its connected issues are labeled `API`',
                    'alt': 'or this PR does not modify a REST API'
                },
                *iif(t is T.default, [
                    {
                        'type': 'cli',
                        'content': 'Added `a` (`A`) tag to commit title for backwards (in)compatible changes',
                        'alt': 'or this PR does not modify a REST API'
                    },
                    {
                        'type': 'cli',
                        'content': 'Updated REST API version number in `app.py`',
                        'alt': 'or this PR does not modify a REST API'
                    }
                ])
            ]),
            *iif(t not in (T.hotfix, T.backport), [
                {
                    'type': 'h2',
                    'content': 'Author (upgrading deployments)'
                },
                *iif(t.target_branch == develop, [
                    {
                        'type': 'cli',
                        'content': 'Documented upgrading of deployments in UPGRADING.rst',
                        'alt': 'or this PR does not require upgrading deployments'
                    },
                    {
                        'type': 'cli',
                        'content': 'Added `u` tag to commit title',
                        'alt': 'or this PR does not require upgrading deployments'
                    },
                    {
                        'type': 'cli',
                        'content': 'Ran `make image_manifests.json` and committed the resulting changes',
                        'alt': 'or this PR does not modify `azul_docker_images`, '
                               'or any other variables referenced in the definition of that variable'
                    }
                ]),
                {
                    'type': 'cli',
                    'content': 'This PR is labeled `upgrade`',
                    'alt': 'or does not require upgrading deployments'
                },
                {
                    'type': 'cli',
                    'content': 'This PR is labeled `deploy:shared`',
                    'alt': 'or does not modify `image_manifests.json`, and does not '
                           'require deploying the `shared` component for any other reason'
                },
                {
                    'type': 'cli',
                    'content': 'This PR is labeled `deploy:gitlab`',
                    'alt': 'or does not require deploying the `gitlab` component'
                },
                {
                    'type': 'cli',
                    'content': 'This PR is labeled `deploy:runner`',
                    'alt': 'or does not require deploying the `runner` image'
                }
            ]),
            *iif(t is T.default, [
                {
                    'type': 'h2',
                    'content': 'Author (operator tasks)'
                },
                {
                    'type': 'cli',
                    'content': 'Added checklist items for additional operator tasks',
                    'alt': 'or this PR does not require additional tasks'
                }]),
            *iif(t in (T.default, T.hotfix), [
                {
                    'type': 'h2',
                    'content': 'Author (hotfixes)'
                },
                *(
                    [
                        {
                            'type': 'cli',
                            'content': 'Added `F` tag to main commit title',
                            'alt': 'or this PR does not include permanent fix for a temporary hotfix'
                        },
                        {
                            'type': 'cli',
                            'content': 'Reverted the temporary hotfixes for any connected issues',
                            'alt': f'or the {bq(prod)} branch has no temporary hotfixes for any connected issues'
                        }
                    ] if t is T.default else [
                        {
                            'type': 'cli',
                            'content': 'Added `h` tag to commit title',
                            'alt': 'or this PR does not include a temporary hotfix'
                        },
                        {
                            'type': 'cli',
                            'content': 'Added `H` tag to commit title',
                            'alt': 'or this PR does not include a permanent hotfix'
                        },
                        {
                            'type': 'cli',
                            'content': 'Added `hotfix` label to PR'
                        },
                        {
                            'type': 'cli',
                            'content': 'This PR is labeled `partial`',
                            'alt': 'or represents a permanent hotfix'
                        },
                    ] if t is T.hotfix else [
                    ]),
            ]),
            *iif(t is not T.promotion, [
                {
                    'type': 'h2',
                    'content': 'Author (before every review)'
                },
                {
                    'type': 'cli',
                    'content': iif(t is T.backport,
                                   f'Merged `{t.target_branch}` into PR branch to integrate upstream changes',
                                   f'Rebased PR branch on `{t.target_branch}`, squashed old fixups')
                },
                {
                    'type': 'cli',
                    'content': 'Ran `make requirements_update`',
                    'alt': 'or this PR does not modify `requirements*.txt`, `common.mk`, `Makefile` and `Dockerfile`'
                },
                {
                    'type': 'cli',
                    'content': 'Added `R` tag to commit title',
                    'alt': 'or this PR does not modify `requirements*.txt`'
                },
                {
                    'type': 'cli',
                    'content': 'This PR is labeled `reqs`',
                    'alt': 'or does not modify `requirements*.txt`'
                },
                iif(t in (T.default, T.upgrade), {
                    'type': 'cli',
                    'content': '`make integration_test` passes in personal deployment',
                    'alt': 'or this PR does not modify functionality that could affect the IT outcome'
                })
            ]),
            *iif(t is T.default, [
                {
                    'type': 'h2',
                    'content': 'Peer reviewer (after requesting changes)'
                },
                {
                    'type': 'p',
                    'content': 'Uncheck the *Author (before every review)* checklists.'
                },
                {
                    'type': 'h2',
                    'content': 'Peer reviewer (after approval)'
                },
                {
                    'type': 'cli',
                    'content': 'PR is not a draft'
                },
                {
                    'type': 'cli',
                    'content': 'Ticket is in *Review requested* column'
                },
                {
                    'type': 'cli',
                    'content': 'Requested review from system administrator'
                },
                {
                    'type': 'cli',
                    'content': 'PR is assigned to system administrator'
                }
            ]),
            *iif(t in (T.default, T.backport), [
                {
                    'type': 'h2',
                    'content': 'System administrator (after requesting changes)'
                },
                {
                    'type': 'p',
                    'content': 'Uncheck the *before every review* checklists. '
                               'Update the `N reviews` label.'
                }
            ]),
            {
                'type': 'h2',
                'content': 'System administrator (after approval)'
            },
            {
                'type': 'cli',
                'content': 'Actually approved the PR'
            },
            iif(t is T.default, {
                'type': 'cli',
                'content': 'Labeled connected issues as `demo` or `no demo`'
            }),
            iif(t is T.upgrade, {
                'type': 'cli',
                'content': 'Labeled connected issue as `no demo`'
            }),
            iif(t is T.default, {
                'type': 'cli',
                'content': 'Commented on connected issues about demo expectations',
                'alt': 'or all connected issues are labeled `no demo`'
            }),
            iif(t is not T.upgrade, {
                'type': 'cli',
                'content': (
                    'Decided if PR can be labeled `no sandbox`'
                    if t in (T.default, T.backport) else
                    'Labeled PR as `no sandbox`'
                )
            }),
            {
                'type': 'cli',
                'content': 'A comment to this PR details the completed security design review',
                'alt': 'or this PR is a promotion or a backport'
            },
            iif(t is not T.promotion, {
                'type': 'cli',
                'content': 'PR title is appropriate as title of merge commit'
            }),
            iif(t is T.default, {
                'type': 'cli',
                'content': '`N reviews` label is accurate'
            }),
            {
                'type': 'cli',
                'content': 'Moved ticket to *Approved* column'
            },
            {
                'type': 'cli',
                'content': 'PR is assigned to current operator'
            },
            {
                'type': 'h2',
                'content': 'Operator (before pushing merge the commit)'
            },
            *iif(t is T.default, [
                {
                    'type': 'cli',
                    'content': 'Checked `reindex:…` labels and `r` commit title tag'
                },
                {
                    'type': 'cli',
                    'content': 'Checked that demo expectations are clear',
                    'alt': 'or all connected issues are labeled `no demo`'
                },
                {
                    'type': 'cli',
                    'content': 'PR has checklist items for upgrading instructions',
                    'alt': 'or PR is not labeled `upgrade`'
                }
            ]),
            iif(t not in (T.promotion, T.backport), {
                'type': 'cli',
                'content': f'Squashed PR branch and rebased onto `{t.target_branch}`'
            }),
            iif(t is not T.promotion, {
                'type': 'cli',
                'content': 'Sanity-checked history'
            }),
            {
                'type': 'cli',
                'content': 'Pushed PR branch to GitHub'
            },
            *iif(t not in (T.backport, T.hotfix), [
                *flatten([
                    [
                        {
                            'type': 'cli',
                            'content': 'Ran ' + bq(
                                f'_select {d}.shared && '
                                f'CI_COMMIT_REF_NAME={t.target_branch} '
                                f'make -C terraform/shared {t.deploy_shared_target}'
                            ),
                            'alt': 'or this PR is not labeled `deploy:shared`'
                        },
                        {
                            'type': 'cli',
                            'content': 'Ran ' + bq(
                                f'_select {d}.gitlab && '
                                f'CI_COMMIT_REF_NAME={t.target_branch} '
                                f'make -C terraform/gitlab apply'
                            ),
                            'alt': 'or this PR is not labeled `deploy:gitlab`'
                        }
                    ]
                    for d in t.target_deployments
                ]),
                {
                    'type': 'cli',
                    'content': 'Checked the items in the next section',
                    'alt': 'or this PR is labeled `deploy:gitlab`'
                },
                {
                    'type': 'cli',
                    'content': 'Assigned system administrator',
                    'alt': 'or this PR is not labeled `deploy:gitlab`'
                },
                {
                    'type': 'h2',
                    'content': 'System administrator'
                },
                *[
                    {
                        'type': 'cli',
                        'content': f'Background migrations for `{d}.gitlab` are complete',
                        'alt': 'or this PR is not labeled `deploy:gitlab`'
                    }
                    for d in t.target_deployments
                ],
                {
                    'type': 'cli',
                    'content': 'PR is assigned to operator',
                },
                {
                    'type': 'h2',
                    'content': 'Operator (before pushing merge the commit)'
                },
                *[
                    {
                        'type': 'cli',
                        'content': 'Ran ' + bq(
                            f'_select {d}.gitlab && '
                            f'make -C terraform/gitlab/runner'
                        ),
                        'alt': 'or this PR is not labeled `deploy:runner`'
                    }
                    for d in t.target_deployments
                ],
            ]),
            iif(any(s is not None for s in t.target_deployments.values()), {
                'type': 'cli',
                'content': 'Added `sandbox` label',
                'alt': iif(t is T.upgrade, None, 'or PR is labeled `no sandbox`')
            }),
            # zip() is used to interleave the steps for each deployment so
            # that first, step 1 is done for all deployments, then step 2
            # for all of them, and so on.
            *flatten(zip(*(
                [
                    {
                        'type': 'cli',
                        'content': f'Pushed PR branch to GitLab `{d}`',
                        'alt': iif(t is T.upgrade, None, 'or PR is labeled `no sandbox`')
                    },
                    {
                        'type': 'cli',
                        'content': f'Build passes in `{s}` deployment',
                        'alt': iif(t is T.upgrade, None, 'or PR is labeled `no sandbox`')
                    },
                    {
                        'type': 'cli',
                        'content': f'Reviewed build logs for anomalies in `{s}` deployment',
                        'alt': iif(t is T.upgrade, None, 'or PR is labeled `no sandbox`')
                    },
                    *iif(t is T.default, [
                        {
                            'type': 'cli',
                            'content': f'Deleted unreferenced indices in `{s}`',
                            'alt': f'or this PR does not remove catalogs '
                                   f'or otherwise causes unreferenced indices in `{d}`'
                        },
                        {
                            'type': 'cli',
                            'content': f'Started reindex in `{s}`',
                            'alt': f'or this PR is not labeled `reindex:{d}`'
                        },
                        {
                            'type': 'cli',
                            'content': f'Checked for failures in `{s}`',
                            'alt': f'or this PR is not labeled `reindex:{d}`'
                        }
                    ])
                ]
                for i, (d, s) in enumerate(t.target_deployments.items())
                if s is not None
            ))),
            {
                'type': 'cli',
                'content': 'The title of the merge commit starts with the title of this PR'
            },
            {
                'type': 'cli',
                'content': 'Added PR # reference '
                           + iif(t is T.backport, '(to this PR) ')
                           + 'to merge commit title'
            },
            {
                'type': 'cli',
                'content': 'Collected commit title tags in merge commit title',
                'alt': iif(t is T.default,
                           'but only include `p` if the PR is labeled `partial`',
                           'but exclude any `p` tags')
            },
            iif(t in (T.default, T.upgrade, T.hotfix), {
                'type': 'cli',
                'content': (
                    'Moved connected issue to *Merged prod* column in ZenHub'
                    if t is t.hotfix else
                    f'Moved connected {t.issues} to Merged column in ZenHub'
                )
            }),
            {
                'type': 'cli',
                'content': 'Pushed merge commit to GitHub'
            },
            *iif(t is T.default, [
                {
                    'type': 'h2',
                    'content': 'Operator (chain shortening)'
                },
                *[
                    {
                        'type': 'cli',
                        'content': content,
                        'alt': 'or this PR is not labeled `base`'
                    }
                    for content in [
                        f'Changed the target branch of the blocked PR to {bq(develop)}',
                        'Removed the `chained` label from the blocked PR',
                        'Removed the blocking relationship from the blocked PR',
                        'Removed the `base` label from this PR'
                    ]
                ]
            ]),
            {
                'type': 'h2',
                'content': 'Operator (after pushing the merge commit)'
            },
            *[
                {
                    'type': 'cli',
                    'content': f'Pushed merge commit to GitLab `{d}`',
                    'alt': iif(t in (T.hotfix, T.promotion, T.upgrade), None, 'or PR is labeled `no sandbox`')
                }
                for d in t.target_deployments
            ],
            *flatten(
                [
                    {
                        'type': 'cli',
                        'content': f'Build passes on GitLab `{d}`'
                                   + iif(t in (T.default, T.backport), '<sup>1</sup>')
                    },
                    {
                        'type': 'cli',
                        'content': f'Reviewed build logs for anomalies on GitLab `{d}`'
                                   + iif(t in (T.default, T.backport), '<sup>1</sup>')
                    }
                ]
                for d, s in t.target_deployments.items()
            ),
            *iif(t.target_branch == develop and t is not T.backport, [
                {
                    'type': 'cli',
                    'content': 'Ran ' + bq(
                        f'_select {d}.shared && '
                        f'make -C terraform/shared apply'
                    ),
                    'alt': 'or this PR is not labeled `deploy:shared`'
                }
                for d in t.target_deployments
            ]),
            {
                'type': 'cli',
                'content': 'Deleted PR branch from GitHub'
            },
            *(
                {
                    'type': 'cli',
                    'content': f'Deleted PR branch from GitLab `{d}`'
                }
                for d, s in t.target_deployments.items()
                if t is not t.promotion
            ),
            *iif(t is T.promotion, [
                {
                    'type': 'cli',
                    'content': 'Moved connected issue to *Merged prod* column on ZenHub'
                },
                {
                    'type': 'cli',
                    'content': 'Moved promoted issues from *Merged* to *Merged prod* column on ZenHub'
                },
                {
                    'type': 'cli',
                    'content': 'Moved promoted issues from *dev* to *prod* column on ZenHub'
                }
            ]),
            iif(t in (T.default, T.backport), {
                'type': 'p',
                'content': '<sup>1</sup> When pushing the merge commit is skipped due to the PR being labelled '
                           '`no sandbox`, the next build triggered by a PR whose merge commit *is* pushed determines '
                           'this checklist item.'
            }),
            *iif(t in (T.default, T.hotfix, T.promotion), [
                {
                    'type': 'h2',
                    'content': 'Operator (reindex)'
                },
                # zip() is used to interleave the steps for each deployment so
                # that first, step 1 is done for all deployments, then step 2
                # for all of them, and so on.
                *flatten(zip(*(
                    [
                        *[
                            {
                                'type': 'cli',
                                'content': f'{action} in `{d}`',
                                'alt': f'or this PR is neither labeled `reindex:partial` nor `reindex:{d}`'
                            } for action in [
                                'Deindexed all unreferenced catalogs',
                                'Deindexed specific sources',
                                'Indexed specific sources'
                            ]
                        ],
                        *[
                            {
                                'type': 'cli',
                                'content': f'{action} in `{d}`',
                                'alt': (
                                    'or neither this PR nor a failed, prior promotion requires it'
                                    if t is T.hotfix else
                                    f'or this PR does not require reindexing `{d}`'
                                )
                            }
                            for action in [
                                'Started reindex',
                                'Checked for, triaged and possibly requeued messages in both fail queues',
                                'Emptied fail queues',
                            ]
                        ]
                    ]
                    for d, s in t.target_deployments.items()
                ))),
                iif(t is T.hotfix, {
                    'type': 'cli',
                    'content': 'Created backport PR and linked to it in a comment on this PR'
                })
            ]),
            {
                'type': 'h2',
                'content': 'Operator'
            },
            iif(t is T.upgrade, {
                'type': 'cli',
                'content': 'Ran `script/export_inspector_findings.py` against `anvilprod`, imported results '
                           'to [Google Sheet](https://docs.google.com/spreadsheets/d/'
                           '1RWF7g5wRKWPGovLw4jpJGX_XMi8aWLXLOvvE5rxqgH8) and posted screenshot of '
                           'relevant<sup>1</sup> findings as a comment on the connected issue.'
            }),
            *iif(t.target_branch == develop and t is not T.backport, [
                {
                    'type': 'cli',
                    'content': 'Propagated the '
                               + join_grammatically(list(map(bq, t.promotion.labels_to_promote)))
                               + ' labels to the next promotion PR',
                    'alt': 'or this PR carries none of these labels'
                },
                {
                    'type': 'cli',
                    'content': 'Propagated any specific instructions related to the '
                               + join_grammatically(list(map(bq, t.promotion.labels_to_promote)))
                               + ' labels from the description of this PR to that of the next promotion PR',
                    'alt': 'or this PR carries none of these labels'
                }
            ]),
            {
                'type': 'cli',
                'content': 'PR is assigned to '
                           + iif(t in (T.upgrade, T.promotion), 'system administrator', 'no one')
            },
            iif(t is T.upgrade, {
                'type': 'p',
                'content': '<sup>1</sup>A relevant finding is a high or critical vulnerability in an image '
                           'that is used within the security boundary. Images not used within the boundary '
                           'are tracked in `azul.docker_images` under a key starting with `_`.'
            }),
            *iif(t in (T.upgrade, T.promotion), [
                {
                    'type': 'h2',
                    'content': 'System administrator'
                },
                iif(t is T.upgrade, {
                    'type': 'cli',
                    'content': 'No currently reported vulnerability requires immediate attention'
                }),
                *[
                    {
                        'type': 'cli',
                        'content': f'Removed unused image tags from [{name} image on DockerHub]({url})',
                        'alt': 'or this promotion does not alter references to that image`'
                    }
                    for name, url in images_we_build_ourselves.items()
                    if t is T.promotion
                ],
                {
                    'type': 'cli',
                    'content': 'PR is assigned to no one'
                },
            ]),
            {
                'type': 'h1',
                'content': 'Shorthand for review comments'
            },
            {
                'type': 'li',
                'content': '`L` line is too long'
            },
            {
                'type': 'li',
                'content': '`W` line wrapping is wrong'
            },
            {
                'type': 'li',
                'content': '`Q` bad quotes'
            },
            {
                'type': 'li',
                'content': '`F` other formatting problem'
            }
        ]
    )


if __name__ == '__main__':
    main()
