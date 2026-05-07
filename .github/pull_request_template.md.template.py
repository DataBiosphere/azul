from enum import (
    Enum,
    auto,
)
from itertools import (
    chain,
)
import json
from pathlib import (
    Path,
)
import re
import sys
import textwrap
from typing import (
    AbstractSet,
    Iterable,
    Mapping,
    Protocol,
    TypedDict,
    cast,
)

from furl import (
    furl,
)
from more_itertools import (
    flatten,
    stagger,
    unzip,
)

from azul import (
    config,
)
from azul.lib import (
    cache,
)
from azul.lib.collections import (
    OrderedSet,
)
from azul.lib.functions import (
    iif,
)
from azul.lib.strings import (
    back_quote as bq,
    join_grammatically,
)
from azul.modules import (
    load_script,
)
from azul.template import (
    emit_text,
)


class Item(TypedDict):
    type: str
    content: str
    alt: str


class EmptyItem(TypedDict):
    pass


class Handler(Protocol):

    def __call__(self, i: Item, j: Item | None) -> Iterable[str]:
        """
        :param i: the checklist item
        :param j: the preceding checklist item
        :return: lines of Markdown code to emit
        """


def emit_checklist(checklist: Iterable[Item | EmptyItem]):
    def comment(i: Item, _) -> Iterable[str]:
        return '<!--', *wrap(i), '-->'

    def p(i: Item, _) -> Iterable[str]:
        return '', *wrap(i)

    def h1(i: Item, _) -> Iterable[str]:
        return '', '', '## ' + text(i['content'])

    def h2(i: Item, _) -> Iterable[str]:
        return '', '', '### ' + text(i['content'])

    def cli(i: Item, j: Item | None) -> Iterable[str]:
        alt = '' if i.get('alt') is None else ' <sub>' + i['alt'] + '</sub>'
        return *margin(i, j), '- [ ] ' + text(i['content']) + alt

    def li(i: Item, j: Item | None) -> Iterable[str]:
        return *margin(i, j), '- ' + i['content']

    handlers = {k: cast(Handler, v) for k, v in locals().items() if callable(v)}

    def margin(i: Item, j: Item | None) -> Iterable[str]:
        return [] if j and j['type'] == i['type'] else ['']

    def wrap(i: Item) -> Iterable[str]:
        return textwrap.wrap(text(i['content']), 80)

    footnotes = {}
    footnote_re = re.compile(r'<footnote ([^/]+)/>')

    def text(content: str) -> str:
        return footnote_re.sub(sup, content)

    def sup(m: re.Match) -> str:
        s = footnotes.setdefault(m.group(1), str(len(footnotes) + 1))
        return '<sup>' + s + '</sup>'

    with emit_text() as f:
        non_empty_items: Iterable[Item] = filter(bool, checklist)
        item_pairs = stagger(non_empty_items, offsets=(0, -1))
        lines = flatten(handlers[i['type']](i, j) for i, j in item_pairs)
        f.writelines(line + '\n' for line in lines)


dir = 'PULL_REQUEST_TEMPLATE'

custom_images = {
    alias: image['url']
    for alias, image in config.docker_images.items()
    if image.get('is_custom') is True
}


class T(Enum):
    default = auto()
    promotion = auto()
    hotfix = auto()
    backport = auto()
    upgrade = auto()

    @property
    def target_branch_by_path(self) -> dict[Path, str]:
        result = {}
        for target_branch in self.target_branches:
            name = 'pull_request_template' if self is T.default else self.name
            name += '.md'
            if target_branch != 'develop':
                name = target_branch + '-' + name
            path = Path(name)
            if self is not T.default:
                path = Path(dir) / path
            assert path not in result, (path, result)
            result[path] = target_branch
        return result

    @property
    def files(self) -> AbstractSet[str]:
        return OrderedSet(p.name for p in self.target_branch_by_path.keys())

    @property
    def target_branches(self) -> AbstractSet[str]:
        return OrderedSet(['anvilprod', 'prod']
                          if self in (T.promotion, T.hotfix) else
                          ['develop'])

    @property
    def issues(self):
        default = self is T.default

        class S(str):

            def __call__(self, then, otherwise):
                return then if default else otherwise

        return S('issue' + iif(default, 's'))

    def target_deployments(self, target_branch: str) -> Mapping[str, str]:
        """
        Returns a mapping between 1) the name of each main deployment the given
        branch can be deployed to and 2) the name of the respective sandbox
        deployment in which PR branches targeting that branch are tested first.
        """
        return {
            'develop': {
                'dev': 'sandbox',
                'anvildev': 'anvilbox'
            },
            'anvilprod': {
                'anvilprod': 'hammerbox'
            },
            'prod': {
                # There currently is no sandbox for production deployments
                'prod': None
            }
        }[target_branch]

    def affected_deployments(self, target_branch: str) -> AbstractSet[str]:
        return OrderedSet(chain(
            self.target_deployments(target_branch).keys(),
            self.downstream_deployments(target_branch)
        ))

    def downstream_deployments(self, target_branch) -> AbstractSet[str]:
        return iif(target_branch == 'develop', OrderedSet(chain.from_iterable(
            self.promotion.target_deployments(b).keys()
            for b in self.promotion.target_branches
        )))

    def has_sandbox_for(self, target_branch: str) -> bool:
        return {None} != set(self.target_deployments(target_branch).values())

    def labels_to_promote(self, target_branch: str) -> AbstractSet[str]:
        return OrderedSet([
            'deploy:shared',
            'deploy:gitlab',
            'deploy:runner',
            *iif(self is T.upgrade, ['backup:gitlab'], [
                'reindex:partial',
                *('reindex:' + d for d in self.downstream_deployments(target_branch)),
                'mirror:partial',
                *('mirror:' + d for d in self.downstream_deployments(target_branch)),
            ])
        ])

    @property
    def needs_shared_deploy(self):
        return self not in (T.backport, T.hotfix)

    # noinspection PyUnusedLocal
    def shared_deploy_is_two_phase(self, target_branch: str) -> bool:
        # All `shared` components are deployed in two-phases. The first phase,
        # prior to the merge, mirrors new images to ECR, while the second phase
        # removes any outdated images. This makes it possible to abandon the
        # merge in case the sandbox build fails. Furthermore, collocated
        # personal deployments would break if the old images were to be deleted
        # immediately. Note that even with two phases, personal deployments will
        # break after the second phase but the fix is simply to rebase any
        # feature branches and redeploy.
        return True

    def shared_deploy_target(self, target_branch: str) -> str:
        return 'apply' + iif(self.shared_deploy_is_two_phase(target_branch), '_keep_unused')


def main():
    path = Path(sys.argv[1])
    for t in T:
        try:
            target_branch = t.target_branch_by_path[path]
        except KeyError:
            pass
        else:
            emit(t, target_branch)


@cache
def deployment_env(deployment: str,
                   component: str | None = None
                   ) -> Mapping[str, str]:
    script = load_script('export_environment')
    deployment += '' if component is None else '.' + component
    env, warning = script.load_env(deployment)
    assert warning is None, warning
    resolved_env = script.resolve_env(env)
    return resolved_env


def azul_domain_name(d):
    return deployment_env(d)['AZUL_DOMAIN_NAME']


def link(anchor: str, url: furl | str) -> str:
    if isinstance(url, furl):
        url = str(url)
    assert ']' not in anchor, anchor
    assert ')' not in url, url
    return f'[{anchor}]({url})'


def emit(t: T, target_branch: str):
    emit_checklist(
        [
            {
                'type': 'comment',
                'content': {
                    T.default: (
                        f'This is the PR template for regular PRs against {bq(target_branch)}. '
                        "Edit the URL in your browser's location bar, appending either " +
                        join_grammatically(
                            [f'`&template={f}`' for tt in T for f in tt.files if tt is not T.default],
                            joiner=', ',
                            last_joiner=' or '
                        ) +
                        ' to switch the template.'
                    ),
                    T.backport: f'This is the PR template for backport PRs against {bq(target_branch)}.',
                    T.upgrade: 'This is the PR template for upgrading Azul dependencies.',
                    T.hotfix: f'This is the PR template for hotfix PRs against {bq(target_branch)}.',
                    T.promotion: f'This is the PR template for a promotion PR against {bq(target_branch)}.'
                }[t]
            },
            iif(t is not T.backport, {
                'type': 'p',
                'content': f'Linked {t.issues}: #0000'
            }),
            {
                'type': 'h1',
                'content': 'Checklist'
            },
            {
                'type': 'h2',
                'content': 'Author'
            },
            {
                'type': 'cli',
                'content': 'PR is assigned to the author'
            },
            {
                'type': 'cli',
                'content': 'Status of PR is *In progress*'
            },
            iif(t is T.default, {
                'type': 'cli',
                'content': 'PR is a draft'
            }),
            {
                'type': 'cli',
                'content': f'Target branch is `{target_branch}`'
            },
            {
                'type': 'cli',
                'content': 'Name of PR branch matches `' + {
                    T.default: 'issues/<GitHub handle of author>/<issue#>-<slug>',
                    T.promotion: f'promotions/yyyy-mm-dd-{target_branch}',
                    T.hotfix: f'hotfixes/<GitHub handle of author>/<issue#>-<slug>-{target_branch}',
                    T.upgrade: 'upgrades/yyyy-mm-dd',
                    T.backport: 'backports/<7-digit SHA1 of most recent backported commit>'
                }[t] + '`'
            },
            iif(t is not t.backport, {
                'type': 'cli',
                'content': {
                    T.default: 'PR is linked to all issues it (partially) resolves',
                    T.upgrade: 'PR is linked to the upgrade issue it resolves',
                    T.hotfix: 'PR is linked to the issue it hotfixes',
                    T.promotion: 'PR is linked to the promotion issue it resolves',
                    T.backport: None
                }[t]
            }),
            {
                'type': 'cli',
                'content': f'Status of linked {t.issues} is ' + (
                    '*In progress*'
                    if t is not T.backport else
                    '*Stable*'
                )
            },
            iif(t is not T.backport, {
                'type': 'cli',
                'content': f'PR description links to linked {t.issues}'
            }),
            iif(t is T.promotion, {
                'type': 'cli',
                'content': 'Title of linked issue matches `Promotion yyyy-mm-dd`'
            }),
            {
                'type': 'cli',
                'content': {
                    t.default: 'PR title matches<footnote title/> that of a linked issue',
                    t.promotion: f'PR title starts with title of linked issue '
                                 f'followed by ` {target_branch}`',
                    t.hotfix: f'PR title is `Hotfix {target_branch}: ` '
                              f'followed by title of linked issue',
                    t.upgrade: 'PR title matches `Upgrade software dependencies yyyy-mm-dd`',
                    t.backport: 'PR title contains the 7-digit SHA1 of the backported commits'
                }[t],
                'alt': iif(t is t.default, "or comment in PR explains why they're different", None)
            },
            iif(t is not T.backport, {
                'type': 'cli',
                'content': f'PR title references {t.issues('all', 'the')} linked {t.issues}'
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
            *iif(t is T.default, [
                {
                    'type': 'cli',
                    'content': 'For each linked issue, there is at least one commit whose title '
                               'references that issue'
                },
                {
                    'type': 'p',
                    'content': '<footnote title/> when the issue title describes a problem, the '
                               'corresponding PR title is `Fix: ` followed by the issue title'
                },
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
                    'alt': 'or completely resolves all linked issues'
                },
                {
                    'type': 'cli',
                    'content': 'This PR partially resolves each of the linked issues',
                    'alt': 'or does not have the `partial` label'
                }
            ]),
            *iif(t in (T.default, T.promotion), [
                {
                    'type': 'h2',
                    'content': 'Author (reindex)'
                },
                iif(t is T.default, {
                    'type': 'cli',
                    'content': 'Added `r` tag to commit title',
                    'alt': 'or the changes introduced by this PR will not require reindexing of any deployment'
                }),
                *[
                    {
                        'type': 'cli',
                        'content': f'This PR is labeled `reindex:{d}`',
                        'alt': f'or the changes introduced by it will not require reindexing of `{d}`'
                    }
                    for d in t.affected_deployments(target_branch)
                ],
                {
                    'type': 'cli',
                    'content': (
                        'This PR is labeled `reindex:partial` and ' +
                        'its description documents the specific reindexing procedure for ' +
                        join_grammatically([
                            f'`{d}`' for d in t.affected_deployments(target_branch)
                        ])
                    ),
                    'alt': (
                        'or requires a full reindex ' +
                        iif(len(t.affected_deployments(target_branch)) == 1,
                            'or is not labeled',
                            'or carries none of the labels ') +
                        join_grammatically([
                            f'`reindex:{d}`'
                            for d in t.affected_deployments(target_branch)
                        ])
                    )
                },
                {
                    'type': 'h2',
                    'content': 'Author (mirror)'
                },
                *[
                    {
                        'type': 'cli',
                        'content': f'This PR is labeled `mirror:{d}`',
                        'alt': f'or the changes introduced by it will not require mirroring of `{d}`'
                    }
                    for d in t.affected_deployments(target_branch)
                ],
                {
                    'type': 'cli',
                    'content': (
                        'This PR is labeled `mirror:partial` and ' +
                        'its description documents the specific mirroring procedure for ' +
                        join_grammatically([
                            f'`{d}`' for d in t.affected_deployments(target_branch)
                        ])
                    ),
                    'alt': (
                        'or requires a full mirroring ' +
                        iif(len(t.affected_deployments(target_branch)) == 1,
                            'or is not labeled',
                            'or carries none of the labels ') +
                        join_grammatically([
                            f'`mirror:{d}`'
                            for d in t.affected_deployments(target_branch)
                        ])
                    )
                },
                *iif(t is T.default, [
                    {
                        'type': 'h2',
                        'content': 'Author (API changes)'
                    },
                    {
                        'type': 'cli',
                        'content': 'This PR and its linked issues are labeled `API`',
                        'alt': 'or this PR does not modify a REST API'
                    },
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
                *iif(target_branch == 'develop', [
                    {
                        'type': 'cli',
                        'content': 'Ran `make docker_images.json` and committed the resulting changes',
                        'alt': 'or this PR does not modify `azul_docker_images`, '
                               'or any other variables referenced in the definition of that variable'
                    },
                    {
                        'type': 'cli',
                        'content': 'Documented upgrading of deployments in UPGRADING.rst',
                        'alt': 'or this PR does not require upgrading deployments'
                    },
                    {
                        'type': 'cli',
                        'content': 'Added `u` tag to commit title',
                        'alt': 'or this PR does not require upgrading deployments'
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
                    'alt': 'or does not modify `docker_images.json`, and does not '
                           'require deploying the `shared` component for any other reason'
                },
                {
                    'type': 'cli',
                    'content': 'This PR is labeled `deploy:gitlab`',
                    'alt': 'or does not require deploying the `gitlab` component'
                },
                iif(t is T.upgrade, {
                    'type': 'cli',
                    'content': 'This PR is labeled `backup:gitlab`',
                }),
                {
                    'type': 'cli',
                    'content': 'This PR is labeled `deploy:runner`',
                    'alt': 'or does not require deploying the `runner` image'
                }
            ]),
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
                            'content': 'Reverted the temporary hotfixes for any linked issues',
                            'alt': 'or the none of the stable branches (' +
                                   join_grammatically(list(map(bq, T.promotion.target_branches))) +
                                   ') have temporary hotfixes for any of the issues linked to this PR'
                        }
                    ]
                    if t is T.default else
                    [
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
                    ]
                    if t is T.hotfix else
                    []
                ),
            ]),
            {
                'type': 'h2',
                'content': 'Author (before every review)'
            },
            {
                'type': 'cli',
                'content': iif(t in (T.backport, T.promotion),
                               (
                                   f'PR branch is up to date (if not, merge `{target_branch}` '
                                   f'into PR branch to integrate upstream changes)'
                               ),
                               f'Rebased PR branch on `{target_branch}`, squashed fixups from prior reviews')
            },
            *iif(target_branch == 'develop' or t is T.hotfix, [
                {
                    'type': 'cli',
                    'content': 'Ran `make requirements_update`',
                    'alt': 'or this PR does not modify ' + join_grammatically(list(map(bq, [
                        'Dockerfile',
                        'environment',
                        'requirements*.txt',
                        'common.mk',
                        'Makefile',
                        'environment.boot',
                    ])), last_joiner=' or ')
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
                iif(t is T.upgrade, {
                    'type': 'cli',
                    'content': 'Updated the `AL2023_release` variable in [gitlab.tf.json.template.py]'
                               '(../blob/develop/terraform/gitlab/gitlab.tf.json.template.py) to the '
                               'most recent [AL2023 release](../blob/develop/OPERATOR.rst#updating-'
                               'software-packages-via-release-version-upgrade-in-al2023-instances)',
                    'alt': 'or no update is available'
                }),
                iif(t not in (T.backport, T.hotfix), {
                    'type': 'cli',
                    'content': '`make integration_test` passes in personal deployment',
                    'alt': 'or this PR does not modify functionality that could affect the IT outcome'
                })
            ]),
            *iif(t is T.default, [
                {
                    'type': 'cli',
                    'content': 'PR is awaiting requested review from a peer'
                },
                {
                    'type': 'cli',
                    'content': 'Status of PR is *Review requested*'
                },
                {
                    'type': 'cli',
                    'content': 'PR is assigned to only the peer and the author'
                },
                {
                    'type': 'h2',
                    'content': 'Peer reviewer (after approval)'
                },
                {
                    'type': 'p',
                    'content': 'Note that after requesting changes, the PR '
                               'must be assigned to only the author.'
                },
                {
                    'type': 'cli',
                    'content': 'Actually approved the PR'
                },
            ]),
            {
                'type': 'cli',
                'content': 'PR is not a draft'
            },
            {
                'type': 'cli',
                'content': 'PR is awaiting requested review from system administrator'
            },
            {
                'type': 'cli',
                'content': 'Status of PR is *Review requested*'
            },
            {
                'type': 'cli',
                'content': 'PR is assigned to only the system administrator and the author'
            },
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
                'content': 'Labeled linked issues as `demo` or `no demo`'
            }),
            iif(t is T.upgrade, {
                'type': 'cli',
                'content': 'Labeled linked issue as `no demo`'
            }),
            iif(t is T.default, {
                'type': 'cli',
                'content': 'Commented on linked issues about demo expectations',
                'alt': 'or all linked issues are labeled `no demo`'
            }),
            iif(t is not T.upgrade, {
                'type': 'cli',
                'content': (
                    'Decided if PR can be labeled `no sandbox`'
                    if t.has_sandbox_for(target_branch) else
                    'Labeled PR as `no sandbox`'
                )
            }),
            iif(t not in (T.backport, T.promotion), {
                'type': 'cli',
                'content': 'A comment to this PR details the completed security design review',
            }),
            iif(t is not T.promotion, {
                'type': 'cli',
                'content': 'PR title is appropriate as title of merge commit'
            }),
            {
                'type': 'cli',
                'content': '`N reviews` label is accurate'
            },
            {
                'type': 'cli',
                'content': 'Status of PR is *Approved*'
            },
            {
                'type': 'cli',
                'content': 'PR is assigned to only the operator and the author'
            },
            {
                'type': 'h2',
                'content': 'Operator'
            },
            *iif(t is T.default, [
                {
                    'type': 'cli',
                    'content': 'Checked `reindex:…` labels and `r` commit title tag'
                },
                {
                    'type': 'cli',
                    'content': 'Checked `mirror:…` labels'
                },
                {
                    'type': 'cli',
                    'content': 'Checked that demo expectations are clear',
                    'alt': 'or all linked issues are labeled `no demo`'
                }
            ]),
            iif(t not in (T.promotion, T.backport), {
                'type': 'cli',
                'content': f'Squashed PR branch and rebased onto `{target_branch}`'
            }),
            iif(t is not T.promotion, {
                'type': 'cli',
                'content': 'Sanity-checked history'
            }),
            {
                'type': 'cli',
                'content': 'Pushed PR branch to GitHub'
            },
            *iif(t.needs_shared_deploy, [
                {
                    'type': 'h2',
                    'content': 'Operator (deploy `.shared` and `.gitlab` components)'
                },
                *flatten([
                    [
                        {
                            'type': 'cli',
                            'content': 'Ran ' + bq(
                                f'_select {d}.shared && '
                                f'CI_COMMIT_REF_NAME={target_branch} '
                                f'make -C terraform/shared {t.shared_deploy_target(target_branch)}'
                            ),
                            'alt': 'or this PR is not labeled `deploy:shared`'
                        },
                        iif(t in (t.upgrade, t.promotion), {
                            'type': 'cli',
                            'content': 'Ran ' + bq(
                                f'_select {d}.gitlab && '
                                f'python scripts/create_gitlab_snapshot.py --no-restart'
                            ) + ' (see [operator manual](../blob/develop/OPERATOR.rst#backup-gitlab-volumes) '
                                'for details)',
                            'alt': 'or this PR is not labeled `backup:gitlab`'
                        }),
                        {
                            'type': 'cli',
                            'content': 'Ran ' + bq(
                                f'_select {d}.gitlab && '
                                f'CI_COMMIT_REF_NAME={target_branch} '
                                f'make -C terraform/gitlab apply'
                            ),
                            'alt': 'or this PR is not labeled `deploy:gitlab`'
                        }
                    ]
                    for d in t.target_deployments(target_branch)
                ]),
                {
                    'type': 'cli',
                    'content': 'Checked the items in the next section',
                    'alt': 'or this PR is labeled `deploy:gitlab`'
                },
                {
                    'type': 'cli',
                    'content': 'PR is assigned to only the system administrator and the author',
                    'alt': 'or this PR is not labeled `deploy:gitlab`'
                },
                {
                    'type': 'h2',
                    'content': 'System administrator (post-deploy of `.gitlab` component)'
                },
                *[
                    {
                        'type': 'cli',
                        'content': f'Background migrations for '
                                   f'[`{d}.gitlab`](https://gitlab.{azul_domain_name(d)}'
                                   f'/admin/background_migrations) are complete',
                        'alt': 'or this PR is not labeled `deploy:gitlab`'
                    }
                    for d in t.target_deployments(target_branch)
                ],
                {
                    'type': 'cli',
                    'content': 'PR is assigned to only the operator and the author',
                },
            ]),
            *iif(t not in (T.hotfix, T.backport), [
                {
                    'type': 'h2',
                    'content': 'Operator (deploy runner image)'
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
                    for d in t.target_deployments(target_branch)
                ]
            ]),
            *iif(t.has_sandbox_for(target_branch), [
                {
                    'type': 'h2',
                    'content': 'Operator (sandbox build)'
                },
                {
                    'type': 'cli',
                    'content': 'Added `sandbox` label',
                    'alt': iif(t is T.upgrade, None, 'or PR is labeled `no sandbox`')
                }
            ]),
            # unzip() is used to interleave the steps for each deployment so
            # that first, step 1 is done for all deployments, then step 2
            # for all of them, and so on.
            *flatten(unzip(
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
                    iif(t not in (T.hotfix, T.backport), {
                        'type': 'cli',
                        'content': f'Applied upgrade instructions from UPGRADING.rst to `{s}`',
                        'alt': f'or this PR is not labeled `upgrade`, '
                               f'or upgrade instructions do not apply to `{s}`'
                    }),
                    *iif(t is not T.upgrade, [
                        {
                            'type': 'cli',
                            'content': f'Deleted unreferenced indices in `{s}`',
                            'alt': f'or this PR does not remove catalogs '
                                   f'or otherwise causes unreferenced indices in `{s}`'
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
                        },
                        {
                            'type': 'cli',
                            'content': f'Started mirroring in `{s}`',
                            'alt': f'or this PR is not labeled `mirror:{d}`'
                        },
                        {
                            'type': 'cli',
                            'content': f'Checked for failures in `{s}`',
                            'alt': f'or this PR is not labeled `mirror:{d}`'
                        },
                    ])
                ]
                for i, (d, s) in enumerate(t.target_deployments(target_branch).items())
                if s is not None
            )),
            {
                'type': 'h2',
                'content': 'Operator (merge the branch)'
            },
            {
                'type': 'cli',
                'content': 'All status checks passed and the PR is mergeable'
            },
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
                           'but only included `p` if the PR is also labeled `partial`',
                           'but excluded any `p` tags')
            },
            iif(t is T.upgrade,
                {
                    'type': 'cli',
                    'content': 'Closed related Dependabot PRs with a comment referencing the corresponding '
                               'commit in this PR',
                    'alt': 'or this PR does not include any such commits'
                }
                ),
            {
                'type': 'cli',
                'content': 'Pushed merge commit to GitHub'
            },
            {
                'type': 'cli',
                'content': f'Status of PR is '
                           f'*Merged {'lower' if target_branch == 'develop' else 'stable'}*'
            },
            iif(target_branch == 'develop' and t is not T.backport, {
                'type': 'cli',
                'content': 'Status of blocked issues is *Triage*',
                'alt': f'or no issues are blocked on the linked {t.issues}'
            }),
            {
                'type': 'h2',
                'content': 'Operator (main build)'
            },
            *[
                {
                    'type': 'cli',
                    'content': f'Pushed merge commit to GitLab `{d}`'
                }
                for d in t.target_deployments(target_branch)
            ],
            *flatten(
                [
                    {
                        'type': 'cli',
                        'content': f'Build passes on GitLab `{d}`'
                    },
                    {
                        'type': 'cli',
                        'content': f'Reviewed build logs for anomalies on GitLab `{d}`'
                    }
                ]
                for d, s in t.target_deployments(target_branch).items()
            ),
            *iif(t not in (T.hotfix, T.backport), [
                *[
                    {
                        'type': 'cli',
                        'content': f'Applied upgrade instructions from UPGRADING.rst to `{d}`',
                        'alt': f'or this PR is not labeled `upgrade`, '
                               f'or upgrade instructions do not apply to `{d}`'
                    }
                    for d in t.target_deployments(target_branch)
                ],
                iif(target_branch == 'develop', {
                    'type': 'cli',
                    'content': 'Notified developers to apply upgrade instructions '
                               'from UPGRADING.rst to their personal deployments',
                    'alt': 'or this PR is not labeled `upgrade`, '
                           'or upgrade instructions do not apply to personal deployments'
                })
            ]),
            *iif(t.needs_shared_deploy and t.shared_deploy_is_two_phase(target_branch), [
                {
                    'type': 'cli',
                    'content': 'Ran ' + bq(
                        f'_select {d}.shared && '
                        f'make -C terraform/shared apply'
                    ),
                    'alt': 'or this PR is not labeled `deploy:shared`'
                }
                for d in t.target_deployments(target_branch)
            ]),
            {
                'type': 'cli',
                'content': 'Deleted PR branch from GitHub'
            },
            {
                'type': 'cli',
                'content': 'PR is assigned to only the operator'
            },
            *(
                {
                    'type': 'cli',
                    'content': f'Deleted PR branch from GitLab `{d}`'
                }
                for d, s in t.target_deployments(target_branch).items()
                if s is not None
            ),
            *(
                [
                    {
                        'type': 'cli',
                        'content': f'Status of linked {t.issues} is ' + (
                            '*Lower*' + iif(t is not T.upgrade, ', or *Triage*, if PR is partial')
                            if target_branch == 'develop' and t is not T.backport else
                            '*Stable*'
                        )
                    }
                ]
                if t is not T.promotion else
                [
                    {
                        'type': 'cli',
                        'content': 'Status of linked issue is *Stable*'
                    },
                    {
                        'type': 'cli',
                        'content': 'Status of promoted<footnote promoted/> PRs is *Merged stable*'
                    },
                    {
                        'type': 'cli',
                        'content': 'Status of promoted<footnote promoted/> issues is *Stable*'
                    },
                    {
                        'type': 'p',
                        'content': '<footnote promoted/> Promoted issues and PRs are referenced in '
                                   'the titles of the commits that the promotion branch introduces to '
                                   'the stable branch. Prior to the promotion, the status of promoted '
                                   'issues (PRs) is *Lower* (*Merged lower*). Promoted PRs in status '
                                   '*Done* do not need to be moved.'
                    }
                ]
            ),
            *iif(t in (T.default, T.hotfix, T.promotion), [
                {
                    'type': 'h2',
                    'content': 'Operator (reindex)'
                },
                # unzip() is used to interleave the steps for each deployment so
                # that first, step 1 is done for all deployments, then step 2
                # for all of them, and so on.
                *flatten(unzip(
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
                                'Emptied fail queues'
                            ]
                        ]
                    ]
                    for d, s in t.target_deployments(target_branch).items()
                )),
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
                    for d, s in t.target_deployments(target_branch).items()
                    for action in [
                        *[
                            ' '.join([
                                'Restarted the Data Browser pipeline for the',
                                link(
                                    browser_site['branch'] + ' branch',
                                    furl(f'https://gitlab.{azul_domain_name(d)}/ucsc/data-browser/-/pipelines/new',
                                         args=dict(ref=browser_site['branch']))
                                ),
                                'on GitLab'
                            ])
                            for browser_site in
                            json.loads(deployment_env(d, 'browser')['azul_browser_sites']).values()
                        ],
                        'Restarted `deploy_browser` job in the GitLab pipeline for this PR'
                    ]
                ],
                iif(t is T.hotfix, {
                    'type': 'cli',
                    'content': 'Created backport PR and linked to it in a comment on this PR'
                })
            ]),
            *iif(t in (T.default, T.hotfix, T.promotion), [
                {
                    'type': 'h2',
                    'content': 'Operator (mirroring)'
                },
                # unzip() is used to interleave the steps for each deployment so
                # that first, step 1 is done for all deployments, then step 2
                # for all of them, and so on.
                *flatten(unzip(
                    [
                        *[
                            {
                                'type': 'cli',
                                'content': f'{action} in `{d}`',
                                'alt': (
                                    f'or neither this PR nor a failed, prior promotion is labelled `mirror:{d}`'
                                    if t is T.hotfix else
                                    f'or this PR is not labelled `mirror:{d}`'
                                )
                            }
                            for action in [
                                'Started mirroring',
                                'Checked for, triaged and possibly requeued messages in mirror fail queue',
                                'Emptied mirror fail queue'
                            ]
                        ]
                    ]
                    for d, s in t.target_deployments(target_branch).items()
                ))
            ]),
            {
                'type': 'h2',
                'content': 'Operator'
            },
            *iif(t is T.upgrade, [
                {
                    'type': 'cli',
                    'content': 'At least 24 hours have passed since `anvildev.shared` was last deployed'
                },
                {
                    'type': 'cli',
                    'content': 'Ran `scripts/export_inspector_findings.py` against `anvildev`, imported results '
                               'to [Google Sheet](https://docs.google.com/spreadsheets/d/'
                               '1RWF7g5wRKWPGovLw4jpJGX_XMi8aWLXLOvvE5rxqgH8) and posted screenshot of '
                               'relevant<footnote relevant/> findings as a comment on the linked issue.'
                }
            ]),
            *iif(target_branch == 'develop' and t is not T.backport, [
                {
                    'type': 'cli',
                    'content': (
                        'Propagated the `upgrade` and `API` labels to the next promotion PRs'
                    ),
                    'alt': 'or this PR carries neither of these labels'
                },
                {
                    'type': 'cli',
                    'content': (
                        'Propagated the ' +
                        join_grammatically(list(map(bq, t.labels_to_promote(target_branch)))) +
                        ' labels to the next promotion PRs'
                    ),
                    'alt': 'or this PR carries none of these labels'
                },
                {
                    'type': 'cli',
                    'content': (
                        'Propagated any specific instructions related to the ' +
                        join_grammatically(list(map(bq, t.labels_to_promote(target_branch)))) +
                        ' labels, from the description of this PR to that of the next promotion PRs'
                    ),
                    'alt': 'or this PR carries none of these labels'
                }
            ]),
            {
                'type': 'cli',
                'content': 'PR is assigned to '
                           + iif(t in (T.upgrade, T.promotion), 'only the system administrator', 'no one')
            },
            iif(t is T.upgrade, {
                'type': 'p',
                'content': '<footnote relevant/>A relevant finding is a high or critical vulnerability in an image '
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
                        'content': f'Removed unused image tags from [{name} image on DockerHub]({url}/tags)',
                        'alt': 'or this promotion does not alter references to that image'
                    }
                    for name, url in custom_images.items()
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
