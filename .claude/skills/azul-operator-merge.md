---
description: "Merge an approved PR into `develop` as the operator for the lower deployments. Covers choosing a deliberate or autonomous cadence, deciding which items are N/A, and the mechanics of ticking, merging, deploying and polling builds."
---

# Operator merge (lower deployments)

Use this skill when asked to merge a PR, do operator work on a PR, or work
through the operator sections of a PR checklist. It covers `dev`, `anvildev`
and their sandboxes only.

## Cadence

This is the part that matters most. The work is a long sequence of small,
mostly irreversible steps, and how much of it to do unattended is the
operator's call, not yours.

**Ask which cadence to use before starting.** Do it once, at the top, before
the first item. Two are established:

- **Deliberate.** Offer one item, wait for approval, perform it, tick it, offer
  the next. Consecutive items that are N/A or satisfiable by a read-only check
  may be offered as one batch with a single approval.

- **Autonomous.** Work through the checklist unattended, stopping only at the
  boundary below or when something deviates from what the checklist assumes.

Deliberate suits an unfamiliar PR, one whose labels or diff look inconsistent,
or a first session with a new operator. Autonomous suits a PR whose shape is
already understood. If the operator does not express a preference, use
deliberate and say that is what you are doing.

The cadence can change mid-PR; the operator will say so. Everything below
applies to both.

### In autonomous cadence, stop and ask for

- any write to `develop` or a stable branch, including each push separately
- force-pushes and branch deletions
- creating the merge commit's title, if no tag census settles it
- `terraform apply`, as opposed to `plan`
- posting anything to Slack, or to GitHub as the operator: comments, issues
- an item whose escape clause turns on the operator's judgement rather than a
  fact you can check
- any deviation: a build failure, a plan showing unexpected drift, a label that
  contradicts the diff, a checklist premise that does not match reality

Everything else proceeds: read-only inspection, polling, deployment switches,
terraform plans, pushing a PR branch to GitLab for sandbox builds, board and
label mutations that the checklist itself prescribes, and ticking items you
have completed.

### In both cadences

**Tick what you complete.** Flip the box in the PR body; reporting an item done
is not enough.

**Justify, don't assert.** For each item, name the evidence: the label that is
absent, the file the diff does not touch, the command output. An item whose
escape clause depends on judgement — "upgrade instructions do not apply to
`sandbox`" — gets the reasoning laid out and the decision left to the operator.

**Separate fact from inference.** Say which is which, every time. If a check
was not run, say so rather than implying it passed. A negative result from a
weak method is weak evidence; say that too.

**Expect to be wrong sometimes.** When a claim turns out false, correct it in a
sentence and move on. Do not quietly drop it.

**The operator may do steps themselves.** They will often say "I'm running that
now". Stand by, then tick the item when they report it done. Do not re-run it
to verify.

**Flag an out-of-scope consequence once.** Then stop raising it.

**Report what you did while unattended.** After a stretch of autonomous work,
say which items were ticked and on what evidence, and surface anything that was
surprising but not blocking. A run of items reported only as a count is not
reviewable.

## Before starting

The board status of the PR must be *Approved*. Anything less — *In Progress*,
*In review* — means the PR is still with the author; say so and stop.

Re-verify state rather than trusting what you remember, especially after a gap:
the checked-out branch, how far `develop` has moved, whether credentials are
still valid, and whether the tooling behaves as it did. Sessions here span days.

## Deciding whether an item applies

Most checklist items carry an escape clause in `<sub>` tags, usually keyed to a
label. The labels are the operator's contract, so check them — but check the
diff too, because a missing label can be an oversight:

| Item family                  | Gating label                        |
|------------------------------|-------------------------------------|
| `.shared` component deploy   | `deploy:shared`                     |
| `.gitlab` component deploy   | `deploy:gitlab`                     |
| Runner image deploy          | `deploy:runner`                     |
| Sandbox build                | `sandbox` / `no sandbox`            |
| Upgrade instructions         | `upgrade`                           |
| Reindex families             | `reindex:partial`, `reindex:<depl>` |
| Mirroring families           | `mirror:dev`, `mirror:anvildev`     |

Two traps:

- **One item inverts.** "Checked the items in the next section" escapes when
  the PR *is* labeled `deploy:gitlab`. Absent the label, the work falls to the
  operator: check the system administrator's section yourself.
- **Reindexing is about the deployed tree, not the commits.** A branch can
  contain an `r`-tagged commit and still require no reindex if a revert in the
  same branch neutralises it. Check what the merge actually changes.

## Mechanics

**Editing the PR body.** Fetch it, modify it, push it back:

```
gh pr view <n> --json body --jq .body > body.md
# edit
gh pr edit <n> --body-file body.md
```

The body uses **CRLF**. Open it with `newline=''` in Python, or every line will
be rewritten. Verify that only checkbox lines changed before pushing.

**Never regenerate the body from the template.** If the template changed, port
its differences into the fetched body. Regenerating destroys edits the operator
made on GitHub — struck-through items, annotations. If that happens, recover
them from `userContentEdits` on the PR via GraphQL.

**Merge commit title.** PR title verbatim, prefixed with the tags collected
from the branch's commit titles, with the PR reference appended inside the
parentheses:

```
[u R] <PR title> (#<issue>, PR #<pr>)
```

Tags belong on commit titles; PRs carry labels instead, so the PR title has no
tag and the operator adds it when composing the merge title. Collect only tags
that describe this merge: `M/N` split ordinals are not tags to collect, and
tags inherited from an unrelated merge commit that rode along are not either.

**Merging.** `git checkout develop && git merge --no-ff <branch> -m "<title>"`.
Verify two parents, that the tree matches the branch tip, and that the working
tree is clean. Push to GitHub, then to both GitLab remotes, then delete the
branch from all three — each after confirming the branch is contained in that
remote's `develop`.

**Board status.** Project #3 (`PVT_kwDOAfSQ384BCJY8`), Status field
`PVTSSF_lADOAfSQ384BCJY8zg0cXCs`. Options: *Merged lower* `0a0d9864`,
*Lower* `cf096c13`, *Triage* `f75ad846`, *In Progress* `47fc9ee4`. Set with
`updateProjectV2ItemFieldValue` and verify by reading the field back.

**Linked issues** come from `closingIssuesReferences`, not from parsing the
`Linked issues:` line. They span repositories.

**Blocked issues** come from the native dependency API, never a text search:

```graphql
issue(number: N) { blocking(first: 20) { nodes { number repository { nameWithOwner } } } }
```

Direct blockees only; ignore blocking relationships among the issues this PR
resolves. Comment on each issue moved to *Triage*:
`Blocker #1234 landed on \`develop\` via PR #2345`, qualifying the blocker
fully when it lives in another repository.

## Technical notes

**`apply_keep_unused` is interactive.** It delegates to `apply`, which has no
`-auto-approve`. Run the read-only `plan` first, show the operator the actions,
then apply via `auto_apply` on their go-ahead. Never pipe `yes` into it. The
plan cannot show what the image-pruning provisioners will delete.

**Expect 23 always-replaced `null_resource`s** in any `terraform/shared` plan;
they carry `triggers = {always = timestamp()}`. Anything beyond that is drift
worth reporting.

**`glab` needs three variables unset** — `GITLAB_TOKEN`, `GITLAB_API_HOST`,
`GITLAB_HOST` — because the azul environment sets them per deployment and they
shadow the per-host keyring credentials:

```
env -u GITLAB_TOKEN -u GITLAB_API_HOST -u GITLAB_HOST glab api --hostname <host> ...
```

**Polling builds.** Sandbox builds report as `gitlab/dev/sandbox` and
`gitlab/anvildev/anvilbox` on the PR; `develop` builds report as
`gitlab/dev/dev` and `gitlab/anvildev/anvildev` on the merge commit. Poll both
in one loop and send exactly one notification: immediately when any build
fails, otherwise once all have passed.

**Notifications must use `terminal-notifier`.** `osascript` is attributed to
`com.apple.ScriptEditor2`, which has no registrable bundle, so macOS presents
it as *none*. The `PushNotification` tool stays silent while the terminal is
active.

**`GIT_SEQUENCE_EDITOR=cat git rebase -i` is not a dry run.** `cat` exits zero,
so git accepts the todo list and performs the rebase. Recover via `ORIG_HEAD`.

**Selecting a deployment may prompt for MFA**, which fails in a non-interactive
shell. Report it and ask the operator to run the selection in a terminal; do
not retry.

**Some items cannot be completed** and should be left unchecked with the reason
stated: an approving review the author cannot give themselves, a linked issue
that only reaches *Stable* on promotion, label propagation to a promotion PR
that does not exist yet, and the final unassignment that waits on those.
