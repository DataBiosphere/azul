# Blue-green deployments (#6026)

## Problem

Reindexing a stable deployment degrades the service for external users: responses are slow, the index is incomplete, manifests fail, and code deployed ahead of the reindex can fail on documents that haven't been reindexed yet (see the 2026-09-24 recurrence in `prod`). The goal is to reindex out of sight of users and switch over once the new index is complete, without permanently doubling the cost of the deployment.

This design uses `anvilprod` as the example. `prod` would follow the same pattern.

## Terminology

A *deployment* (`anvilprod`) consists of two *instances*, `anvilprod1` and `anvilprod2`. Each instance is in one of three states:

- *live*: serves the public domain names
- *standby*: fully provisioned, reachable only at its canonical domain names
- *dormant*: its expensive resources, most importantly the OpenSearch domain, don't exist

Exactly one instance is live at any time. A live instance is never dormant. Bringing a dormant instance back is called *waking* it.

Each instance has *canonical* domain names, such as `service.1.explore.anvilproject.org`, which always lead to that instance. The deployment has *public* domain names, such as `service.explore.anvilproject.org`, which lead to whichever instance is live.

## Terraform components

| Component | `anvilprod` | `anvilprod1`, `anvilprod2` |
|---|---|---|
| main | alias mode: public domain names only | full instance |
| `base` | ? | per instance (KMS keys, GCP service account) |
| `shared` | single, as today | — |
| `gitlab` | single, as today | — |
| `browser` | single, as today | — |

In *alias mode*, the main component owns only the public domain names: the ACM certificates, the API Gateway custom domain names and the Route 53 A records. It doesn't own the base path mappings that connect a public custom domain name to an instance's API. Those are maintained by the `go_live` job.

The instances' main components are configured like any other main deployment, in `deployments/anvilprod1/environment.py` and `deployments/anvilprod2/environment.py`, with `AZUL_SUBDOMAIN_TEMPLATE` set to `*.1` or `*.2`, and the same `AZUL_DOMAIN_NAME` as `anvilprod`. All resource names are qualified with `AZUL_DEPLOYMENT_STAGE`, so the two instances don't collide. These files also exist on `develop`. Which instance a build deploys is not configured in the source (see *Branches and pipelines*).

### Why not alias records

Switching the public names by repointing DNS alias records doesn't work with edge-optimized API Gateway custom domain names. A custom domain name can exist only once, so both instances can't own `service.explore.anvilproject.org`. CloudFront also routes by the `Host` header, so a request for the public name reaching the distribution of `service.1.…` would most likely be rejected. This hasn't been tested, but the existing DRS alias also needed a custom domain name of its own, not just a DNS record.

Switching the base path mapping (`update-base-path-mapping` with patch operations on `/restapiId` and `/stage`) avoids that problem. It takes effect without waiting for a DNS TTL to expire, and switching back is the same operation.

### `shared` and the ECR images

`shared` remains single. It mirrors upstream images into ECR and, on every regular `apply`, deletes the images that the current commit doesn't use. The two-phase `shared` deploy that already exists (`apply_keep_unused`, then `apply`) covers this. The second phase may run only from a commit that every instance is on, except for dormant ones.

The Lambda image repository is unaffected. Its tags are qualified with the stage (`anvilprod1-service`, …), and `--delete-unused` doesn't touch it.

## State

The instance states live in a single SSM parameter per deployment, such as `/azul/anvilprod/instances`:

```json
{"live": "1", "dormant": {"1": false, "2": true}}
```

Storing the live instance as a single value guarantees that exactly one instance is live. The scripts that write the parameter enforce the remaining rule that a live instance must not be dormant: making the live instance dormant is refused, and so is making a dormant instance live. SSM has no compare-and-swap, so every job that writes the parameter is placed in the GitLab `resource_group: anvilprod`, which makes GitLab run them one at a time.

SSM is authoritative. The base path mappings are derived from it, and `go_live` reconciles them with it, so re-running `go_live` repairs a switch that failed halfway.

GitLab CI/CD variables were considered. `CI_JOB_TOKEN` can't write them: the variables API isn't listed among the APIs a job token can access, nor among those covered by fine-grained job token permissions. Writing them would require a stored personal access token. The deployment's pipelines can already write to SSM.

### Effect on `deploy`

`deploy` reconciles an instance's resources with its state. For a dormant instance, the OpenSearch domain (and the alarms that depend on it, as in `_hibernate`) are omitted from the Terraform configuration, so applying it destroys them. For any other instance, they are created. Making an instance dormant or waking it therefore takes two steps: change the state, then deploy. `deploy` refuses if the state is inconsistent.

The integration test, `early_reindex` and `reindex` short-circuit for dormant instances.

## Branches and pipelines

Promotion and hotfix PRs target the `anvilprod` branch, as they do today. After the merge, the operator pushes the merge commit to GitLab twice, under different branch names:

```
git push gitlab.anvilprod anvilprod
git push gitlab.anvilprod anvilprod:anvilprod1
```

The name of the branch being built selects the instance:

```python
'azul_shared_deployments': json.dumps({
    …
    'anvilprod': ['anvilprod', 'anvilprod1', 'anvilprod2', 'hammerbox'],
    'anvilprod1': ['anvilprod1'],
    'anvilprod2': ['anvilprod2'],
})
```

The `anvilprod` entry lets local builds on the `anvilprod` branch select any of the three deployments. The other two entries make GitLab deploy the instance named by the branch being built.

| Job | `anvilprod` pipeline | `anvilprod1`, `anvilprod2` pipelines |
|---|---|---|
| `test` | runs | skipped |
| `deploy_base`, `deploy` | alias mode | instance |
| `deploy_browser` | runs | no-op (no `deployments/anvilprodN.browser`) |
| `integration_test` | skipped | runs against the canonical names |
| `early_reindex`, `reindex` | absent | manual |
| `go_live` | absent | manual |

Jobs short-circuit based on the configuration (`if python -m azul -t config.deployment.is_alias …`). The manual jobs use `rules:` on `$CI_COMMIT_BRANCH` instead, so that no button appears that does nothing when clicked.

The instance pipelines skip `test` because the same commit is tested in the `anvilprod` pipeline. GitLab doesn't link the two pipelines, so the checklist makes the operator wait for `test` in the `anvilprod` pipeline before pushing the instance branch.

### `go_live`

1. Refuse if this instance is dormant or its index is incomplete. The index is complete when every catalog's indices exist and the indexer queues are empty. `needs: [integration_test]` covers the integration test.
2. Record this instance as live in SSM.
3. Point each public custom domain name's base path mapping at this instance's API and stage.
4. Check `/version` and `/health/basic` through the public names.

Running `go_live` in the other instance's pipeline switches back.

## Integration test

The integration test needs no changes. It runs in the instance's pipeline, with the instance's configuration, so `config.service_endpoint` and `config.indexer_endpoint` resolve to the canonical names, and every AWS resource it touches is qualified with the instance's stage. The OAuth flow in the attended mode of the test redirects to a local HTTP server, so the canonical names don't need to be registered with Google as redirect URIs.

The test doesn't cover the public path, from the public custom domain name through the base path mapping to the instance. The check at the end of `go_live` and the QC period cover it.

## Self-referencing URLs

During a request, the service derives its base URL from the `Host` header (`AzulChaliceApp.base_url`). A user who calls the public name receives public URLs, and the integration test, which calls the canonical name, receives canonical ones. This needs no changes.

Outside a request, `base_url` falls back to `config.lambda_endpoint`, which will be the canonical name. The compact and curl manifests use that fallback: they are generated in Step Functions and embed `/repository/files` URLs. Users would receive manifests that refer to the canonical name, and those URLs would break once that instance becomes dormant after a later switch.

The fix is to capture the base URL of the request that starts the manifest's Step Functions execution, include it in the `ManifestKey` (and therefore in the manifest hash), and have `generate()` build its URLs from the key rather than from `app.base_url`. Including the base URL in the hash also keeps a manifest generated on a standby instance from being served from the cache after `go_live`. Taking the value from the key also prevents a race with `go_live` between starting the execution and generating the manifest.

Any other code that builds URLs outside of a request would read the SSM parameter (with a short cache) and use the public name when its instance is live and the canonical name otherwise. Whether any such code exists besides the manifest generators hasn't been checked.

URLs that refer to state held by one instance, such as cached manifest objects and manifest fetch tokens, break after a switch no matter which name they use, because each instance has its own storage bucket. How long such URLs need to stay valid limits how soon the old instance may be made dormant.

## Promotions

A promotion switches instances if it requires a full reindex, a catalog-wide reindex, or the addition of a catalog. A catalog-wide reindex affects the performance that all external users observe, and so does adding a catalog. Every other promotion, and every hotfix, is deployed to the live instance in place.

The `reindex:` labels currently can't distinguish a catalog-wide reindex from a partial one: `reindex:partial` covers both. A `reindex:catalog` label or a narrower definition of `reindex:partial` is needed.

A dormant instance comes back without any OpenSearch indices, so a switch always indexes every catalog from scratch. Adding `anvil17` to a deployment with `anvil16` means indexing both. That is acceptable for now. Restoring an unchanged catalog's indices from a snapshot of the live domain would be an optimization for promotions that change neither the index schema nor the aggregation logic.

### In place

Precondition: the other instance is dormant. This rules out an in-place promotion during the QC period of a switch.

1. Phase 1 of the `shared` deploy (`apply_keep_unused`)
2. Merge, push `anvilprod`, wait for `test`
3. Push `anvilprod:anvilprodL`, where L is the live instance
4. Partial reindex in the `anvilprodL` pipeline, if one is required
5. Phase 2 of the `shared` deploy (`apply`). The dormant instance's Lambda images aren't in the mirrored repositories, and it will be brought to the current commit before it is woken.

### Switch

L is the live instance, S the dormant instance that becomes live.

1. Wake S in SSM
2. Phase 1 of the `shared` deploy (`apply_keep_unused`)
3. Merge, push `anvilprod`, wait for `test`
4. Push `anvilprod:anvilprodS`. `deploy` creates the OpenSearch domain and the integration test runs.
5. `early_reindex` or `reindex` in the `anvilprodS` pipeline
6. `go_live` in the `anvilprodS` pipeline
7. QC period. L is still on the previous commit with a complete index, so rolling back is a matter of running `go_live` in the latest `anvilprodL` pipeline.
8. Mark L dormant in SSM, then push `anvilprod:anvilprodL`. `deploy` destroys L's OpenSearch domain, and the integration test short-circuits. This is the point of no return. After it, rolling back requires another switch.
9. Phase 2 of the `shared` deploy (`apply`). All instances and `shared` are on the same commit.

Steps 1 and 8 change the state before the push. Whether they are done by a script that the operator runs locally or by a manual job in some pipeline is still open.

## PR templates

The steps are spelled out verbatim, with instance names, in one template per instance and mode, for example:

- `anvilprod1-promotion.md`, `anvilprod2-promotion.md`: in place
- `anvilprod1-switch.md`, `anvilprod2-switch.md`: switch to that instance
- `anvilprod1-hotfix.md`, `anvilprod2-hotfix.md`

All of them still say "Target branch is `anvilprod`". `scripts/pull_request.py` already derives the template from the name of the promotion branch (`promotions/<date>-<name>` → `<name>-promotion.md`) and the target branch from the item in the template, so a branch name such as `promotions/2026-11-21-anvilprod2` works without changes to the script. The template generator needs reworking: it derives the file name and the target deployments from the target branch (`T.target_branch_by_path`, `T.target_deployments`), which would have to be keyed by instance instead.

## Open questions

- Canonical domain names: `service.1.explore.anvilproject.org` (`*.1`) or `service.anvilprod1.explore.anvilproject.org` (`*.{AZUL_DEPLOYMENT_STAGE}`, the convention used by hammerbox)
- Does the alias-mode `anvilprod` need a `base` component, or do only the instances have one?
- Who changes the state in steps 1 and 8 of a switch: a local script or a manual job?
- Naming of the templates and of the promotion branches that select them
- Alarms and scheduled Lambdas of a dormant instance. Its Lambdas and API remain, but requests to them fail without an OpenSearch domain.
- Whether the public custom domain names behave as assumed, specifically that the base path mapping of an edge-optimized custom domain name can be switched between two APIs in the same account and region without downtime
- Other code paths outside of a request that hand out URLs
- Do the Data Browser or Swagger UI need the canonical names registered as OAuth redirect URIs, for use against a standby instance?
- Contents and duration of the QC period
- Migration of the existing `anvilprod` main component into `anvilprod1`, and of its public domain names into the alias-mode component, without downtime
- `reindex:` label for catalog-wide reindexing
