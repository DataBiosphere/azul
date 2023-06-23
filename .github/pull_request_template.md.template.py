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


class T(Enum):
    default = 'pull_request_template.md'
    promotion = dir + '/promotion.md'
    hotfix = dir + '/hotfix.md'
    backport = dir + '/backport.md'
    gitlab = dir + '/gitlab.md'

    @property
    def file(self):
        return basename(self.value)

    @property
    def dir(self):
        return dirname(self.value)

    @property
    def target_branch(self):
        return 'prod' if self in (T.promotion, T.hotfix) else 'develop'

    @property
    def issues(self):
        default = self is T.default

        class S(str):

            def __call__(self, then, otherwise):
                return then if default else otherwise

        return S('issue' + iif(default, 's'))

    @property
    def gitlab_deployments(self):
        return OrderedSet(chain.from_iterable(t.deployments for t in type(self)))

    @property
    def deployments(self) -> Mapping[str, str]:
        """
        Maps the name of the each deployment to that of the respective sandbox.
        """
        return (
            {
                'prod': None  # There is no sandbox for production
            }
            if self in (T.promotion, T.hotfix) else
            {}  # the `gitlab` component is deployed manually
            if self is T.gitlab else
            {
                'dev': 'sandbox',
                'anvildev': 'anvilbox',
                # No sandbox for anvilprod, either, even though it's a defacto
                # non-production deployment at the moment
                'anvilprod': None
            }
        )


def main():
    t = one(tt for tt in T if tt.value == sys.argv[1])
    emit_checklist(
        [
            {
                'type': 'comment',
                'content': {
                    T.default: 'This is the PR template for regular PRs against `develop`. '
                               "Edit the URL in your browser's location bar, appending either "
                               + join_grammatically([f'`&template={tt.file}`' for tt in T if tt.dir],
                                                    joiner=', ',
                                                    last_joiner=' or ')
                               + ' to switch the template.',
                    T.backport: 'This is the PR template for backport PRs against `develop`.',
                    T.gitlab: 'This is the PR template for upgrading the GitLab instance.',
                    T.hotfix: 'This is the PR template for hotfix PRs against `prod`.',
                    T.promotion: 'This is the PR template for promotion PRs against `prod`.'
                }[t]
            },
            iif(t is not T.backport, {
                'type': 'p',
                'content': f'Connected {t.issues}: #' + iif(t is T.gitlab, '4014', '0000')

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
                    T.gitlab: 'gitlab/yyyy-mm-dd/<GitLab version>',
                    T.backport: 'backports/<7-digit SHA1 of most recent backported commit>'
                }[t] + '`'
            },
            iif(t is T.promotion, {
                'type': 'cli',
                'content': 'Title of connected issue matches `Promotion yyyy-mm-dd`'
            }),
            iif(t not in (T.backport, T.gitlab), {
                'type': 'cli',
                'content': f"PR title references {t.issues('all', 'the')} connected {t.issues}"
            }),
            {
                'type': 'cli',
                'content': {
                    t.default: 'PR title matches<sup>1</sup> that of a connected issue',
                    t.promotion: 'PR title starts with title of connected issue',
                    t.hotfix: 'PR title is `Hotfix: ` followed by title of connected issue',
                    t.gitlab: 'PR title matches `Update GitLab to <GitLab version> (#4014)`',
                    t.backport: 'PR title contains the 7-digit SHA1 of the backported commits'
                }[t],
                'alt': iif(t is t.default, "or comment in PR explains why they're different", None)
            },
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
            iif(t is T.gitlab, {
                'type': 'cli',
                'content': 'Disconnected any other PRs currently connected to #4014 via ZenHub'
            }),
            iif(t is not t.backport, {
                'type': 'cli',
                'content': {
                    T.default: 'PR is connected to all connected issues via ZenHub',
                    T.gitlab: 'PR is connected to issue #4014 via ZenHub',
                    T.hotfix: 'PR is connected to issue via ZenHub',
                    T.promotion: 'PR is connected to issue via ZenHub',
                    T.backport: None
                }[t]
            }),
            iif(t not in (T.backport, T.gitlab), {
                'type': 'cli',
                'content': f'PR description links to connected {t.issues}'
            }),
            iif(t is T.default, {
                'type': 'cli',
                'content': 'Added `partial` label to PR',
                'alt': 'or this PR completely resolves all connected issues'
            }),
            iif(t is T.default, {
                'type': 'p',
                'content': '<sup>1</sup> when the issue title describes a problem, the corresponding PR title is '
                           '`Fix: ` followed by the issue title'
            }),
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
                {
                    'type': 'cli',
                    'content': 'Added `reindex` label to PR',
                    'alt': 'or this PR does not require reindexing'
                },
                {
                    'type': 'cli',
                    'content': 'PR and connected issue are labeled `API`',
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
            *iif(t is T.default, [
                {
                    'type': 'h2',
                    'content': 'Author (chains)'
                },
                {
                    'type': 'cli',
                    'content': 'This PR is blocked by previous PR in the chain',
                    'alt': 'or this PR is not chained to another PR'
                },
                {
                    'type': 'cli',
                    'content': 'Added `base` label to the blocking PR',
                    'alt': 'or this PR is not chained to another PR'
                },
                {
                    'type': 'cli',
                    'content': 'Added `chained` label to this PR',
                    'alt': 'or this PR is not chained to another PR'
                }
            ]),
            *iif(t in (T.default, T.promotion), [
                {
                    'type': 'h2',
                    'content': 'Author (upgrading)'
                },
                iif(t is T.default, {
                    'type': 'cli',
                    'content': 'Documented upgrading of deployments in UPGRADING.rst',
                    'alt': 'or this PR does not require upgrading'
                }),
                iif(t is T.default, {
                    'type': 'cli',
                    'content': 'Added `u` tag to commit title',
                    'alt': 'or this PR does not require upgrading'
                }),
                {
                    'type': 'cli',
                    'content': 'Added `upgrade` label to PR',
                    'alt': 'or this PR does not require upgrading'
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
                            'alt': 'or the `prod` branch has no temporary hotfixes for any connected issues'
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
                            'content': 'Added `partial` label to PR',
                            'alt': 'or this PR is a permanent hotfix'
                        },
                    ] if t is T.hotfix else [
                    ]),
            ]),
            *iif(t not in (T.gitlab, T.promotion), [
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
                    'alt': 'or this PR does not touch requirements*.txt, common.mk, Makefile and Dockerfile'
                },
                {
                    'type': 'cli',
                    'content': 'Added `R` tag to commit title',
                    'alt': 'or this PR does not touch requirements*.txt'
                },
                {
                    'type': 'cli',
                    'content': 'Added `reqs` label to PR',
                    'alt': 'or this PR does not touch requirements*.txt'
                },
                iif(t is T.default, {
                    'type': 'cli',
                    'content': '`make integration_test` passes in personal deployment',
                    'alt': 'or this PR does not touch functionality that could break the IT'
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
                    'content': 'Requested review from primary reviewer'
                },
                {
                    'type': 'cli',
                    'content': 'Assigned PR to primary reviewer'
                }
            ]),
            *iif(t is T.gitlab, [
                {
                    'type': 'h2',
                    'content': 'Author (deploy)'
                },
                *[
                    {
                        'type': 'cli',
                        'content': f'Deployed changes to `{deployment}.gitlab`'
                    } for deployment in t.gitlab_deployments
                ]
            ]),
            *iif(t in (T.default, T.backport), [
                {
                    'type': 'h2',
                    'content': 'Primary reviewer (after requesting changes)'
                },
                {
                    'type': 'p',
                    'content': 'Uncheck the *before every review* checklists. '
                               'Update the `N reviews` label.'
                }
            ]),
            {
                'type': 'h2',
                'content': 'Primary reviewer (after approval)'
            },
            *iif(t is T.gitlab, [
                {
                    'type': 'cli',
                    'content': f'Verified background migrations for `{d}.gitlab` are complete'
                } for d in t.gitlab_deployments
            ]),
            {
                'type': 'cli',
                'content': 'Actually approved the PR'
            },
            iif(t is T.default, {
                'type': 'cli',
                'content': 'Labeled connected issues as `demo` or `no demo`'
            }),
            iif(t is T.gitlab, {
                'type': 'cli',
                'content': 'Labeled connected issue as `no demo`'
            }),
            iif(t is T.default, {
                'type': 'cli',
                'content': 'Commented on connected issues about demo expectations',
                'alt': 'or all connected issues are labeled `no demo`'
            }),
            {
                'type': 'cli',
                'content': (
                    'Decided if PR can be labeled `no sandbox`'
                    if t in (T.default, T.backport) else
                    'Labeled PR as `no sandbox`'
                )
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
                'content': 'Assigned PR to current operator'
            },
            {
                'type': 'h2',
                'content': 'Operator (before pushing merge the commit)'
            },
            *iif(t is T.default, [
                {
                    'type': 'cli',
                    'content': 'Checked `reindex` label and `r` commit title tag'
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
            # zip() is used to interleave the steps for each deployment so
            # that first, step 1 is done for all deployments, then step 2
            # for all of them, and so on.
            *flatten(zip(*(
                [
                    {
                        'type': 'cli',
                        'content': f'Pushed PR branch to GitLab `{d}`'
                                   + iif(i == 0, ' and added `sandbox` label'),
                        'alt': 'or PR is labeled `no sandbox`'
                    },
                    {
                        'type': 'cli',
                        'content': f'Build passes in `{s}` deployment',
                        'alt': 'or PR is labeled `no sandbox`'
                    },
                    {
                        'type': 'cli',
                        'content': f'Reviewed build logs for anomalies in `{s}` deployment',
                        'alt': 'or PR is labeled `no sandbox`'
                    },
                    *iif(t is T.default, [
                        {
                            'type': 'cli',
                            'content': f'Deleted unreferenced indices in `{s}`',
                            'alt': 'or this PR does not remove catalogs or otherwise causes unreferenced indices '
                        },
                        {
                            'type': 'cli',
                            'content': f'Started reindex in `{s}`',
                            'alt': 'or this PR does not require reindexing `sandbox`'
                        },
                        {
                            'type': 'cli',
                            'content': f'Checked for failures in `{s}`',
                            'alt': 'or this PR does not require reindexing `sandbox`'
                        }
                    ])
                ]
                for i, (d, s) in enumerate(t.deployments.items())
                if s is not None
            ))),
            {
                'type': 'cli',
                'content': 'Title of merge commit starts with title from this PR'
            },
            {
                'type': 'cli',
                'content': f"Added PR reference {iif(t is T.backport, '(this PR) ')}to merge commit title"
            },
            {
                'type': 'cli',
                'content': 'Added commit title tags to merge commit title'
            },
            iif(t in (T.default, T.gitlab, T.hotfix), {
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
            *iif(t in (T.default, T.gitlab), [
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
                        'Changed the target branch of the blocked PR to `develop`',
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
                    'alt': iif(t in (T.hotfix, T.promotion), None, 'or PR is labeled `no sandbox`')
                }
                for d in t.deployments
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
                for d, s in t.deployments.items()
            ),
            {
                'type': 'cli',
                'content': 'Deleted PR branch from GitHub'
            },
            *(
                {
                    'type': 'cli',
                    'content': f'Deleted PR branch from GitLab `{d}`'
                }
                for d, s in t.deployments.items()
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
                        {
                            'type': 'cli',
                            'content': f'Deleted unreferenced indices in `{d}`',
                            'alt': 'or this PR does not remove catalogs or otherwise causes unreferenced indices '
                        },
                        {
                            'type': 'cli',
                            'content': f'Started reindex in `{d}`',
                            'alt': (
                                'or neither this PR nor a prior failed promotion requires it'
                                if t is T.hotfix else
                                'or this PR does not require reindexing'
                            )
                        },
                        {
                            'type': 'cli',
                            'content': f'Checked for and triaged indexing failures in `{d}`',
                            'alt': (
                                'or neither this PR nor a prior failed promotion requires it'
                                if t is T.hotfix else
                                'or this PR does not require reindexing'
                            )

                        },
                        {
                            'type': 'cli',
                            'content': f'Emptied fail queues in `{d}` deployment',
                            'alt': (
                                'or neither this PR nor a prior failed promotion requires it'
                                if t is T.hotfix else
                                'or this PR does not require reindexing'
                            )
                        }
                    ]
                    for d, s in t.deployments.items()
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
            {
                'type': 'cli',
                'content': 'Unassigned PR'
            },
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
