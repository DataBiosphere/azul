---
name: Update the ZAP scan context file
about: Template for the annual review/update of the web application vulnerability scan (DAST) context file
title: Update the ZAP scan context file
labels: compliance,infra,no demo,operator
type: Chore
_priority: Medium
_repository: DataBiosphere/azul-private
_start: 2026-01-01T09:00
_period: 1 year
---
- [ ] Export the `Default Context` provided by ZAP to a temporary file
- [ ] Compare the exported file with [azul-zap-scan.context](https://github.com/DataBiosphere/azul-private/blob/main/azul-zap-scan.context) and apply any relevant changes to the latter
- [ ] Open a new PR with the resulting changes, if any

Relevant changes are those that add entries to the `<tech>` element, or that enable additional features. The `<alertFilters>` element should generally be left as is, because it controls what findings we deem false positives.
