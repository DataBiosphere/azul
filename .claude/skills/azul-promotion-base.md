---
description: "Find the develop commit that a stable branch was last promoted from. Works for `prod`, `anvilprod`, or any branch that follows the promotion merge workflow."
---

# Promotion base

Use this skill when asked to find the develop commit that was promoted to a
stable branch, or to determine what version of develop a stable branch is based
on.

When the user specifies multiple stable branches, apply the procedure to each
one independently and report results for all of them.

## Background

A promotion merges a promotion branch (e.g., `promotions/2026-08-15-prod`) into
the target stable branch. The promotion branch is created from a commit on
`develop`, then the target stable branch is merged into it (to reconcile
history), and optionally hotfixes are applied on top. The resulting merge commit
on the stable branch has:

- Parent 1: the previous tip of the stable branch
- Parent 2: the tip of the promotion branch

The promotion branch itself contains a merge commit titled "Merge branch
'<target>' into promotions/<date>-<target>". That merge's first parent is the
develop commit the promotion was based on.

## Procedure

Apply the following steps to each target stable branch (e.g., `prod`,
`anvilprod`):

1. Get the two parents of the tip commit on the target branch:

   ```
   git cat-file -p <target-branch>
   ```

   Parent 1 is the previous stable tip. Parent 2 is the promotion branch tip.

2. Walk parent 2's history to find the merge of the target branch into the
   promotion branch. Look for a commit message matching "Merge branch
   '<target>' into promotions/":

   ```
   git log --oneline <parent2> --grep="Merge branch '<target>' into promotions/"
   ```

3. Get the parents of that merge commit:

   ```
   git cat-file -p <merge-commit>
   ```

   Parent 1 is the develop commit the promotion was based on.

4. Verify the result is on `develop`:

   ```
   git branch --contains <develop-commit> | grep -w develop
   ```

## Pitfall: backported commits

Commits that originated on a stable branch and were backported to `develop`
appear in `git log develop` and pass `git branch --contains`, but they are not
develop-native commits. Their parents include prior promotion merge commits
(e.g., a previous "Promotion ... anvilprod" commit as a parent). Always trace
through the promotion branch merge to find the true develop base rather than
relying on `git merge-base` or `git log` alone.
