---
description: "Move issues in Stable with the `demoed` or `no demo` label to Done on the GitHub project board."
---

# Close demoed issues

Use this skill when asked to close demoed issues, move demoed issues to Done,
or sweep the Stable column.

## What it does

Finds all issues in status *Stable* on the Azul project board that carry the
`demoed` or `no demo` label, and sets their status to *Done*.

## Label distinction

`demo` and `demoed` are distinct labels. Only `demoed` (already demonstrated)
and `no demo` (demonstration not required) qualify. Issues labeled only `demo`
(demonstration expected but not yet done) must not be moved.

## GitHub project coordinates

- Organization: `DataBiosphere`
- Project number: `3`
- Project node ID: `PVT_kwDOAfSQ384BCJY8`
- Status field ID: `PVTSSF_lADOAfSQ384BCJY8zg0cXCs`
- *Done* option ID: `98236657`

## Procedure

1. Query all project items in status *Stable* using paginated GraphQL with
   `query: "status:Stable"` on `ProjectV2.items`. Fetch each item's content
   type, issue number, title, and labels.

2. Filter to issues (not PRs) whose labels include `demoed` or `no demo`.

3. For each matching issue, update the Status field to *Done*.

4. Verify by re-querying that no matching issues remain in *Stable*.

5. Report the count and list of moved issues.

## Implementation notes

### Querying items in Stable

```
gh api graphql --paginate -f query='
query($endCursor: String) {
  organization(login: "DataBiosphere") {
    projectV2(number: 3) {
      items(first: 100, after: $endCursor, query: "status:Stable") {
        pageInfo { hasNextPage endCursor }
        nodes {
          id
          content {
            __typename
            ... on Issue {
              number title
              labels(first: 50) { nodes { name } }
            }
          }
          fieldValueByName(name: "Status") {
            ... on ProjectV2ItemFieldSingleSelectValue { name }
          }
        }
      }
    }
  }
}'
```

Filter results client-side: `fieldValueByName.name == "Stable"`,
`content.__typename == "Issue"`, and labels include `demoed` or `no demo`.

### Mutation

Use **`-f`** (not `-F`) for all variables to avoid numeric coercion of option
IDs.

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
   -f option=98236657
```
