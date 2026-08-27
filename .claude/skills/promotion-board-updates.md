---
description: "Update the GitHub project board after a promotion merge. Moves promoted PRs to Merged stable and promoted issues to Stable, per the promotion PR checklist."
---

# Promotion board updates

Use this skill when the operator asks to update the project board after a
promotion, or when asked to set the status of promoted PRs or issues. The
procedure applies to both `prod` and `anvilprod` promotions.

## Definitions

**Promoted commits** are the commits that the merge commit introduces to the
stable branch. They are listed by:

```
git log --format='%h %s' <merge-commit> --not <first-parent-of-merge-commit>
```

where `<first-parent-of-merge-commit>` is the first SHA in:

```
git log --format='%P' -1 <merge-commit>
```

**Promoted issues and PRs** are those referenced in the titles of promoted
commits. A `#N` reference is an issue; a `PR #N` reference is a PR. The
promotion issue and PR themselves (referenced in the merge commit title) are
excluded from the promoted set.

## Status transitions

Per `.github/PULL_REQUEST_TEMPLATE/prod-promotion.md` (lines 108-114) and
`.github/PULL_REQUEST_TEMPLATE/anvilprod-promotion.md` (lines 123-129):

| Item type | Expected prior status | Target status  | Exception           |
|-----------|-----------------------|----------------|----------------------|
| PR        | Merged lower          | Merged stable  | Already *Done* → skip |
| Issue     | Lower                 | Stable         | Not *Lower* → skip  |

## GitHub project coordinates

- Organization: `DataBiosphere`
- Project number: `3`
- Project node ID: `PVT_kwDOAfSQ384BCJY8`
- Status field ID: `PVTSSF_lADOAfSQ384BCJY8zg0cXCs`

### Status option IDs

| Status         | Option ID  |
|----------------|------------|
| Merged lower   | 0a0d9864   |
| Merged stable  | 1dc9bc5c   |
| Lower          | cf096c13   |
| Stable         | d2915005   |
| Done           | 98236657   |

## Procedure

1. Identify the merge commit. Find its two parents:

   ```
   git log --format='%h %P' -1 <merge-commit>
   ```

2. List promoted commits (exclude the merge itself and any internal merge):

   ```
   git log --format='%h %s' <merge-commit> --not <first-parent>
   ```

3. Extract `#N` and `PR #N` references from commit titles. Separate into
   issues and PRs. Exclude the promotion issue and PR (those appear in the
   merge commit title itself).

4. For each promoted PR, query its project item ID and current status via
   `gh api graphql`. Skip if status is already *Done*. Otherwise set status
   to *Merged stable*.

5. For each promoted issue, query its project item ID and current status.
   Skip if status is not *Lower* (the issue may still be open, tracked
   independently, or already moved). Otherwise set status to *Stable*.

6. Verify by re-querying that all transitions succeeded and report a summary
   table.

## Implementation notes

### GraphQL mutation for status changes

Use `gh api graphql` with **`-f`** (not `-F`) for all variables. `-F` coerces
numeric-looking option IDs (like `98236657`) to integers, causing the mutation
to reject them.

```
gh api graphql -f query='
mutation($item: ID!, $project: ID!, $field: ID!, $option: String!) {
  updateProjectV2ItemFieldValue(input: {
    projectId: $project, itemId: $item, fieldId: $field,
    value: { singleSelectOptionId: $option }
  }) { projectV2Item { id } }
}' -f project=PVT_kwDOAfSQ384BCJY8 \
   -f item=<ITEM_ID> \
   -f field=PVTSSF_lADOAfSQ384BCJY8zg0cXCs \
   -f option=<OPTION_ID>
```

### Querying an issue or PR's project item

```
gh api graphql -f query='
query($n: Int!) {
  repository(owner: "DataBiosphere", name: "azul") {
    issue(number: $n) {
      number
      projectItems(first: 10) {
        nodes {
          id
          project { number }
          fieldValueByName(name: "Status") {
            ... on ProjectV2ItemFieldSingleSelectValue { name }
          }
        }
      }
    }
  }
}' -F n=<NUMBER>
```

Use `pullRequest(number: $n)` instead of `issue(number: $n)` for PRs.
