#!/usr/bin/env bash
# Manages migration of the Priority field from a GitHub Projects V2 project to
# GitHub's native Priority issue field for all issues in a given repository.
#
# Modes:
#   update   — copy project Priority to native Priority (sets native field)
#   validate — verify that native Priority matches the project Priority
#   cleanup  — clear the project Priority after native Priority is confirmed set
#
# Requires: gh CLI (authenticated), jq, bash 4+
#
# Required token scopes (gh auth refresh -s <scope>):
#   repo        — read repo metadata; read/write native issue field values
#   read:org    — list org-level issue fields to resolve the Priority field ID
#   project     — query project V2 items and clear project field values
#                 (read:project alone is insufficient; cleanup requires write access)
set -euo pipefail

usage() {
    cat >&2 <<'EOF'
Usage: migrate_priorities.bash --repo OWNER/REPO --project NUMBER --mode MODE [--issue NUMBER] [--status STATUS] [--dry-run] [--verbose]

  --repo OWNER/REPO     Repository whose issues will be migrated (required)
  --project NUMBER      Project V2 number holding the current Priority field (required)
  --mode MODE           Operation mode: update, validate, or cleanup (required)
  --issue NUMBER        Process only this issue number (for testing)
  --status open|closed  Process only open or only closed issues (default: all)
  --dry-run             Preview changes without applying them (update and cleanup only)
  --verbose             Show per-issue progress; default is summary only
EOF
    exit 1
}

REPO=""
PROJECT_NUMBER=""
MODE=""
ISSUE_NUMBER=""
STATUS=""
DRY_RUN=false
VERBOSE=false

while [[ $# -gt 0 ]]; do
    case "$1" in
        --repo)    REPO="$2";           shift 2 ;;
        --project) PROJECT_NUMBER="$2"; shift 2 ;;
        --mode)    MODE="$2";           shift 2 ;;
        --issue)   ISSUE_NUMBER="$2";   shift 2 ;;
        --status)  STATUS="$2";         shift 2 ;;
        --dry-run) DRY_RUN=true;        shift   ;;
        --verbose) VERBOSE=true;        shift   ;;
        *)         usage ;;
    esac
done

[[ -z "$REPO" ]]           && { echo "Error: --repo is required" >&2;    usage; }
[[ -z "$PROJECT_NUMBER" ]] && { echo "Error: --project is required" >&2; usage; }
[[ -z "$MODE" ]]           && { echo "Error: --mode is required" >&2;    usage; }
[[ "$REPO" != */* ]]       && { echo "Error: --repo must be OWNER/REPO" >&2; exit 1; }

case "$MODE" in
    update|validate|cleanup) ;;
    *) echo "Error: --mode must be one of: update, validate, cleanup" >&2; exit 1 ;;
esac

case "$STATUS" in
    open|closed|"") ;;
    *) echo "Error: --status must be 'open' or 'closed'" >&2; exit 1 ;;
esac

ORG="${REPO%%/*}"
REPO_NAME="${REPO##*/}"

# fd 3 carries verbose output: open to stdout when --verbose is set, /dev/null otherwise
if [[ "$VERBOSE" == "true" ]]; then
    exec 3>&1
else
    exec 3>/dev/null
fi

# Map project field option names to native Priority option names
normalize_priority() {
    case "$1" in
        "--") echo "Low"    ;;
        "-")  echo "Medium" ;;
        "+")  echo "High"   ;;
        "++") echo "Urgent" ;;
        *)    echo ""       ;;
    esac
}

echo "=== Priority Migration — $MODE ===" >&3
echo "Repository: $REPO" >&3
echo "Project:    #$PROJECT_NUMBER" >&3
if [[ "$DRY_RUN" == "true" ]]; then
    [[ "$MODE" == "validate" ]] \
        && echo "(--dry-run ignored for validate)" >&3 \
        || echo "(dry-run — no changes will be made)" >&3
fi
echo "" >&3

# The write endpoint requires the numeric repository ID, not owner/repo.
# Fetching it unconditionally also serves as an early validity check on --repo.
echo "Fetching repository metadata..." >&3
REPO_ID=$(gh api "/repos/$REPO" --jq '.id')
echo "  Repo ID: $REPO_ID" >&3

echo "Fetching org issue fields for '$ORG'..." >&3
PRIORITY_FIELD_ID=$(gh api "/orgs/$ORG/issue-fields" \
    -H "X-GitHub-Api-Version: 2026-03-10" \
    --jq '.[] | select(.name == "Priority") | .id')
if [[ -z "$PRIORITY_FIELD_ID" ]]; then
    echo "Error: no 'Priority' field found in org '$ORG'" >&2
    echo "  Ensure issue fields are enabled and the Priority field exists." >&2
    exit 1
fi
echo "  Priority field ID: $PRIORITY_FIELD_ID" >&3

echo "Fetching project node ID for #$PROJECT_NUMBER..." >&3
PROJECT_NODE_ID=$(gh api graphql \
    -f query='query($org:String!,$number:Int!){organization(login:$org){projectV2(number:$number){id}}}' \
    -f org="$ORG" \
    -F number="$PROJECT_NUMBER" \
    --jq '.data.organization.projectV2.id')
if [[ -z "$PROJECT_NODE_ID" ]]; then
    echo "Error: project #$PROJECT_NUMBER not found in org '$ORG'" >&2
    exit 1
fi
echo "  Project node ID: $PROJECT_NODE_ID" >&3
echo "" >&3

# GraphQL queries: fetch issues from the target repository with their project
# field values inline. Querying by repo (rather than by project) avoids paging
# through items from unrelated repos in large cross-repo projects.
#
# For --issue NUMBER a single API call suffices; no pagination is needed.
# For all issues, paginate through the repository's own issue list.
SINGLE_ISSUE_QUERY='query($owner: String!, $repo: String!, $number: Int!) {
  repository(owner: $owner, name: $repo) {
    issue(number: $number) {
      number
      state
      projectItems(first: 20) {
        nodes {
          id
          project { number }
          fieldValues(first: 20) {
            nodes {
              __typename
              ... on ProjectV2ItemFieldSingleSelectValue {
                name
                field { ... on ProjectV2SingleSelectField { id name } }
              }
            }
          }
        }
      }
    }
  }
}'

ISSUES_QUERY='query($owner: String!, $repo: String!, $cursor: String) {
  repository(owner: $owner, name: $repo) {
    issues(first: 100, after: $cursor) {
      pageInfo { hasNextPage endCursor }
      nodes {
        number
        state
        projectItems(first: 20) {
          nodes {
            id
            project { number }
            fieldValues(first: 20) {
              nodes {
                __typename
                ... on ProjectV2ItemFieldSingleSelectValue {
                  name
                  field { ... on ProjectV2SingleSelectField { id name } }
                }
              }
            }
          }
        }
      }
    }
  }
}'

CLEAR_MUTATION='mutation($projectId: ID!, $itemId: ID!, $fieldId: ID!) {
  clearProjectV2ItemFieldValue(input: {
    projectId: $projectId
    itemId: $itemId
    fieldId: $fieldId
  }) {
    projectV2Item { id }
  }
}'

# Collect issues into a temp file (one JSON object per line) so that counter
# variables updated inside the processing loop remain in scope afterwards.
# priority/item_id/field_id are null for issues not in the target project.
TMPFILE=$(mktemp)
trap 'rm -f "$TMPFILE"' EXIT

check_gql_errors() {
    if echo "$1" | jq -e '.errors' > /dev/null 2>&1; then
        echo "Error: GraphQL query failed:" >&2
        echo "$1" | jq -r '.errors[].message' >&2
        exit 1
    fi
}

page=1

if [[ -n "$ISSUE_NUMBER" ]]; then
    echo "Fetching issue #$ISSUE_NUMBER from $REPO..." >&3
    result=$(gh api graphql \
        -f query="$SINGLE_ISSUE_QUERY" \
        -f owner="$ORG" \
        -f repo="$REPO_NAME" \
        -F number="$ISSUE_NUMBER")
    check_gql_errors "$result"

    echo "$result" | jq -c --arg status "$STATUS" --arg pnum "$PROJECT_NUMBER" '
      .data.repository.issue |
      select(. != null) |
      select(($status == "") or ((.state | ascii_downcase) == $status)) |
      . as $issue |
      ([.projectItems.nodes[] | select(.project.number == ($pnum | tonumber))] | first // null) as $pitem |
      {
        number:   $issue.number,
        priority: (if $pitem == null then null else ([$pitem.fieldValues.nodes[] | select(.__typename == "ProjectV2ItemFieldSingleSelectValue" and .field.name == "Priority") | .name] | first // null) end),
        item_id:  (if $pitem == null then null else $pitem.id end),
        field_id: (if $pitem == null then null else ([$pitem.fieldValues.nodes[] | select(.__typename == "ProjectV2ItemFieldSingleSelectValue" and .field.name == "Priority") | .field.id] | first // null) end)
      }' >> "$TMPFILE"
else
    echo "Fetching $REPO issues (paginated)..." >&3
    cursor=null
    while true; do
        result=$(gh api graphql \
            -f query="$ISSUES_QUERY" \
            -f owner="$ORG" \
            -f repo="$REPO_NAME" \
            -F cursor="$cursor")
        check_gql_errors "$result"

        echo "$result" | jq -c --arg status "$STATUS" --arg pnum "$PROJECT_NUMBER" '
          .data.repository.issues.nodes[] |
          select(($status == "") or ((.state | ascii_downcase) == $status)) |
          . as $issue |
          ([.projectItems.nodes[] | select(.project.number == ($pnum | tonumber))] | first // null) as $pitem |
          {
            number:   $issue.number,
            priority: (if $pitem == null then null else ([$pitem.fieldValues.nodes[] | select(.__typename == "ProjectV2ItemFieldSingleSelectValue" and .field.name == "Priority") | .name] | first // null) end),
            item_id:  (if $pitem == null then null else $pitem.id end),
            field_id: (if $pitem == null then null else ([$pitem.fieldValues.nodes[] | select(.__typename == "ProjectV2ItemFieldSingleSelectValue" and .field.name == "Priority") | .field.id] | first // null) end)
          }' >> "$TMPFILE"

        has_next=$(echo "$result" | jq -r '.data.repository.issues.pageInfo.hasNextPage')
        [[ "$has_next" != "true" ]] && break
        cursor=$(echo "$result" | jq -r '.data.repository.issues.pageInfo.endCursor')
        page=$((page + 1))
    done
fi

total=$(wc -l < "$TMPFILE" | tr -d ' ')
echo "  Found $total issue(s) in $REPO (across $page API call(s))" >&3
echo "" >&3

case "$MODE" in

  update)
    migrated=0
    skipped_no_project_priority=0
    skipped_unmapped=0
    skipped_already_set=0
    failed=0

    while IFS= read -r item; do
        [[ -z "$item" ]] && continue
        number=$(echo "$item"           | jq -r '.number')
        project_priority=$(echo "$item" | jq -r '.priority')

        if [[ "$project_priority" == "null" ]]; then
            skipped_no_project_priority=$((skipped_no_project_priority + 1))
            continue
        fi

        priority=$(normalize_priority "$project_priority")
        if [[ -z "$priority" ]]; then
            printf "  #%s: unrecognized project priority '%s' — skipping\n" \
                "$number" "$project_priority" >&3
            skipped_unmapped=$((skipped_unmapped + 1))
            continue
        fi

        if [[ "$DRY_RUN" == "true" ]]; then
            printf "  #%-6s project Priority='%s' → native Priority='%s'\n" \
                "$number" "$project_priority" "$priority" >&3
            migrated=$((migrated + 1))
            continue
        fi

        # Skip if native Priority is already set (idempotent re-runs)
        existing=$(gh api "/repos/$REPO/issues/$number/issue-field-values" \
            -H "X-GitHub-Api-Version: 2026-03-10" \
            --jq "map(select(.issue_field_id == $PRIORITY_FIELD_ID)) | .[0].single_select_option.name // empty" \
            2>/dev/null || true)

        if [[ -n "$existing" ]]; then
            printf "  #%s: native Priority already set to '%s' — skipping\n" \
                "$number" "$existing" >&3
            skipped_already_set=$((skipped_already_set + 1))
            continue
        fi

        payload=$(jq -n \
            --argjson fid "$PRIORITY_FIELD_ID" \
            --arg     val "$priority" \
            '{"issue_field_values": [{"field_id": $fid, "value": $val}]}')

        if echo "$payload" | gh api "/repositories/$REPO_ID/issues/$number/issue-field-values" \
            -X POST \
            -H "X-GitHub-Api-Version: 2026-03-10" \
            --input - > /dev/null 2>&1; then
            printf "  #%s: set native Priority='%s'\n" "$number" "$priority" >&3
            migrated=$((migrated + 1))
        else
            printf "ERROR: #%s: failed to set native Priority\n" "$number" >&2
            failed=$((failed + 1))
        fi

        sleep 0.1
    done < "$TMPFILE"

    echo ""
    echo "=== Update Summary: $REPO ==="
    [[ "$DRY_RUN" == "true" ]] && echo "  (dry-run: no changes made)"
    printf "  %-38s %d\n" "Migrated:"                        "$migrated"
    printf "  %-38s %d\n" "Skipped (no project priority):"   "$skipped_no_project_priority"
    printf "  %-38s %d\n" "Skipped (unmapped):"              "$skipped_unmapped"
    printf "  %-38s %d\n" "Skipped (already set):"           "$skipped_already_set"
    printf "  %-38s %d\n" "Failed:"                          "$failed"
    ;;

  validate)
    matched=0
    missing_unexpected=0
    mismatched=0
    no_project_priority=0

    while IFS= read -r item; do
        [[ -z "$item" ]] && continue
        number=$(echo "$item"           | jq -r '.number')
        project_priority=$(echo "$item" | jq -r '.priority')

        if [[ "$project_priority" == "null" ]]; then
            no_project_priority=$((no_project_priority + 1))
            continue
        fi

        expected=$(normalize_priority "$project_priority")
        if [[ -z "$expected" ]]; then
            printf "  #%s: unrecognized project priority '%s' — skipping\n" \
                "$number" "$project_priority" >&3
            continue
        fi

        native=$(gh api "/repos/$REPO/issues/$number/issue-field-values" \
            -H "X-GitHub-Api-Version: 2026-03-10" \
            --jq "map(select(.issue_field_id == $PRIORITY_FIELD_ID)) | .[0].single_select_option.name // empty" \
            2>/dev/null || true)

        if [[ -z "$native" ]]; then
            printf "  #%s: MISSING native Priority (project: '%s', expected '%s')\n" \
                "$number" "$project_priority" "$expected" >&3
            missing_unexpected=$((missing_unexpected + 1))
        elif [[ "$native" == "$expected" ]]; then
            printf "  #%s: OK (project: '%s' → native: '%s')\n" \
                "$number" "$project_priority" "$native" >&3
            matched=$((matched + 1))
        else
            printf "  #%s: MISMATCH (project: '%s' → expected '%s', got '%s')\n" \
                "$number" "$project_priority" "$expected" "$native" >&3
            mismatched=$((mismatched + 1))
        fi

        sleep 0.1
    done < "$TMPFILE"

    echo ""
    echo "=== Validation Summary: $REPO ==="
    printf "  %-38s %d\n" "Matched:"                                  "$matched"
    printf "  %-38s %d\n" "Missing — unexpected (has project priority):"  "$missing_unexpected"
    printf "  %-38s %d\n" "Missing — expected (no project priority):"     "$no_project_priority"
    printf "  %-38s %d\n" "Mismatched:"                               "$mismatched"
    ;;

  cleanup)
    cleaned=0
    skipped_not_set=0
    skipped_mismatch=0
    skipped_unmapped=0
    failed=0

    while IFS= read -r item; do
        [[ -z "$item" ]] && continue
        number=$(echo "$item"           | jq -r '.number')
        project_priority=$(echo "$item" | jq -r '.priority')
        item_id=$(echo "$item"          | jq -r '.item_id')
        field_id=$(echo "$item"         | jq -r '.field_id')

        expected=$(normalize_priority "$project_priority")
        if [[ -z "$expected" ]]; then
            printf "  #%s: unrecognized project priority '%s' — skipping\n" \
                "$number" "$project_priority" >&3
            skipped_unmapped=$((skipped_unmapped + 1))
            continue
        fi

        # Only clean up if the native Priority is confirmed set and correct,
        # to avoid leaving an issue with no priority at all
        native=$(gh api "/repos/$REPO/issues/$number/issue-field-values" \
            -H "X-GitHub-Api-Version: 2026-03-10" \
            --jq "map(select(.issue_field_id == $PRIORITY_FIELD_ID)) | .[0].single_select_option.name // empty" \
            2>/dev/null || true)

        if [[ -z "$native" ]]; then
            printf "  #%s: native Priority not set — skipping cleanup\n" "$number" >&3
            skipped_not_set=$((skipped_not_set + 1))
            continue
        fi

        if [[ "$native" != "$expected" ]]; then
            printf "  #%s: native Priority mismatch (expected '%s', got '%s') — skipping cleanup\n" \
                "$number" "$expected" "$native" >&3
            skipped_mismatch=$((skipped_mismatch + 1))
            continue
        fi

        if [[ "$DRY_RUN" == "true" ]]; then
            printf "  #%-6s would clear project Priority='%s' (native confirmed: '%s')\n" \
                "$number" "$project_priority" "$native" >&3
            cleaned=$((cleaned + 1))
            continue
        fi

        if ! clear_result=$(gh api graphql \
            -f query="$CLEAR_MUTATION" \
            -f projectId="$PROJECT_NODE_ID" \
            -f itemId="$item_id" \
            -f fieldId="$field_id"); then
            printf "ERROR: #%s: failed to clear project Priority\n" "$number" >&2
            failed=$((failed + 1))
            continue
        fi

        if echo "$clear_result" | jq -e '.errors' > /dev/null 2>&1; then
            printf "ERROR: #%s: %s\n" "$number" \
                "$(echo "$clear_result" | jq -r '.errors[].message' | head -1)" >&2
            failed=$((failed + 1))
            continue
        fi

        printf "  #%s: cleared project Priority='%s'\n" "$number" "$project_priority" >&3
        cleaned=$((cleaned + 1))

        sleep 0.1
    done < "$TMPFILE"

    echo ""
    echo "=== Cleanup Summary: $REPO ==="
    [[ "$DRY_RUN" == "true" ]] && echo "  (dry-run: no changes made)"
    printf "  %-30s %d\n" "Cleaned:"              "$cleaned"
    printf "  %-30s %d\n" "Skipped (not set):"    "$skipped_not_set"
    printf "  %-30s %d\n" "Skipped (mismatch):"   "$skipped_mismatch"
    printf "  %-30s %d\n" "Skipped (unmapped):"   "$skipped_unmapped"
    printf "  %-30s %d\n" "Failed:"               "$failed"
    ;;

esac
