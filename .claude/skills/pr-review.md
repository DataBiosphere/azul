---
name: pr-review
description: "Review an Azul PR"
user_invocable: true
---

# PR review

Use this skill when asked to review a PR, to check whether a PR actually fixes
its linked issues, or to post review findings to GitHub.

## Ground rules

- Ask for confirmation before proceeding with a review of a PR whose branch is
  either A) not checked out locally, B) locally checked out but not up-to-date
  with the PR HEAD or C) has uncommitted changes locally

- Do not post anything to GitHub until explicitly asked. Report the verdict in
  chat first. A request to review is not a request to post.

- Never submit a review event. Approving, requesting changes or commenting is
  the user's call, not yours; see step 4.

- Distinguish fact from assumption in every finding. "Confirmed: 6 of 6
  requests returned 500" and "I suspect this path is unguarded" are different
  claims and must read differently.

- Cite locations as full `file:line`, never a bare line number.

## Step 1: General evaluation

Perform whatever analysis you see fit. 

## Step 2: Routine checks

- **Checklist audit.** Compare the description's checked items against
  reality: labels actually present on the PR, `p`/`r`/`a`/`A`/`u`/`R`/`F` tags
  actually on commit titles, the `app.py` API version bump against whether the
  change is backwards compatible, and whether a minor bump is defensible. Tags
  go on commit titles; PRs carry labels. Report mismatches — a checked box
  asserting state that does not exist is itself a finding.

- **Reindex and mirror labels.** Analyse the actual changes rather than
  applying these mechanically.

## Step 3: Report the verdict

Report in chat before touching GitHub. Separate blocking from non-blocking,
most severe first. Every blocking finding states the claim, the evidence, and
how it was verified. Say plainly what passed, too — lint, type check and test
status belong in the report even when they are clean, because their being
clean is part of the story.

## Step 4: Post as a pending review, only when asked

### Pending versus published

Omitting `event` from the review POST creates a **PENDING** review: the
comments are drafts, visible only to their author, exactly as when composing a
review in the web UI. The user submits it themselves.

```
gh api repos/DataBiosphere/azul/pulls/<N>/reviews --method POST --input review.json \
  -q '"review id: \(.id)  state: \(.state)"'
```

Confirm the response says `state: PENDING`. Caveats:

- Only one pending review per user per PR.

- The review is attributed to the authenticated account; check
  `gh api user -q .login` and tell the user, since the review will read as
  theirs even though each comment carries the Claude Code attribution.

- To discard, `DELETE /pulls/<N>/reviews/<review_id>`.

### Anchoring inline comments

Inline comments only anchor to lines that appear in the PR's diff. The locally
checked out branch *should* be up-to-date and the working copy *should* be
clean but just to be sure, compute anchors from GitHub's diff:

```
gh pr diff <N> | awk '
/^\+\+\+ /   { path = substr($0, 7); next }
/^@@/        { match($0, /\+[0-9]+/); n = substr($0, RSTART + 1, RLENGTH - 1) + 0; next }
/^[-\\]/     { next }
/^\+/        { printf "%s:%d + %s\n", path, n, substr($0, 2); n++; next }
/^ /         { n++; next }'
```

Then verify the content at each chosen anchor before posting, where
`<headRefOid>` comes from `gh pr view <N> --json headRefOid -q .headRefOid`:

```
git show <headRefOid>:<path> | sed -n '<start>,<end>p'
```

Line numbers cited in the comment *text* must also be PR-head numbers, so
verify those the same way. Further rules:

- Pass `commit_id` explicitly, set to the PR head.

- Multi-line comments use `start_line` plus `line`, with `start_side` and
  `side` both `RIGHT`.

- Warn the user that a force-push to the PR branch before they submit will
  mark the comments outdated, and that re-anchoring means deleting and
  recreating the review. This holds regardless of the state of the local
  checkout.

### Post conventions

- `*(Posted by Claude Code)*` as the first line of the review body **and** of
  every inline comment; each comment is a separate post.

- No hard-coded line breaks; let GitHub wrap.

- Build the payload with the Write tool into a JSON file and pass it via
  `gh api --input`. Do not inline it as a heredoc.

- Fold closely related points into one comment on a shared anchor rather than
  stacking several comments on adjacent lines.
