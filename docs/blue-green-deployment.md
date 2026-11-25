# Blue-green deployments (#6026)


## Problem

Reindexing a stable deployment degrades the service for external users: responses are slow, the index is incomplete, manifests fail, and code deployed ahead of the reindex can fail on documents that haven't been reindexed yet (see the 2026-09-24 recurrence in `prod`). The goal is to reindex out of sight of users and switch over once the new index is complete, without permanently doubling the cost of the deployment.

This design uses `anvilprod` as the example. `prod` would follow the same pattern.


## Terminology

A *deployment* (`anvilprod`) consists of two *instances*, `anvilprod1` and `anvilprod2`. Each instance is in one of three states:

- *live*: serves the public domain names

- *standby*: fully provisioned, reachable only at its canonical domain names

- *resting*: its expensive resources, most importantly the OpenSearch domain, don't exist

Exactly one instance is live at any time. A live instance never rests. Deploying a resting instance *wakes* it.

Each instance has *canonical* domain names, such as `service.1.explore.anvilproject.org`, which always lead to that instance. The deployment has *public* domain names, such as `service.explore.anvilproject.org`, which lead to whichever instance is live.


## Configuration

The deployments that take part set `AZUL_BLUE_GREEN_MODE` to `1`. All other deployments leave it at its default of `0` and behave exactly as before. Its name begins with `AZUL_` so that it is exported to the Lambdas, which need it at runtime (see *Self-referencing URLs*).

When the mode is on, the name of the deployment determines its role. A name without a numeric suffix, such as `anvilprod`, denotes the alias. A name with one, such as `anvilprod1`, denotes the instance of that number. There are always exactly two instances, `1` and `2`. Components take the role of the deployment they belong to: `anvilprod1.base` is the `base` component of instance 1. When the mode is off, a numeric suffix has no meaning, as in the existing personal deployment `hannes2`.

Each of the three deployments has its own `environment.py`. The three files are kept in sync manually, as is already done for hammerbox and anvilprod, or for a personal deployment and the sandbox it is based on:

- `deployments/anvilprod1/environment.py` and `deployments/anvilprod2/environment.py` must be identical except for `AZUL_DEPLOYMENT_STAGE` and `AZUL_SUBDOMAIN_TEMPLATE`. This is stricter than the relationship between hammerbox and anvilprod: if a promotion that adds a catalog changed only the file of the instance it switches to, the other instance would come back without that catalog when it is woken for the next switch.

- `deployments/anvilprod/environment.py` configures the alias, whose main component needs little more than the domain names. However, the `shared`, `gitlab` and `browser` components inherit from it through the existing component mechanism, so whatever they read must remain in that file.

A unit test enforces the first rule. For every deployment `D` that has `AZUL_BLUE_GREEN_MODE` set to `1` and no numeric suffix, the test loads `env()` from `deployments/D1/environment.py` and `deployments/D2/environment.py`, removes `AZUL_DEPLOYMENT_STAGE` and `AZUL_SUBDOMAIN_TEMPLATE` from both, and asserts that the remainders are equal. It also asserts that both instance directories exist and that both set `AZUL_BLUE_GREEN_MODE` to `1`. A forgotten sync thereby fails the build instead of breaking an instance at the next switch.


## Terraform components

| Component | `anvilprod` | `anvilprod1`, `anvilprod2` |
|---|---|---|
| main | alias mode: public domain names only | full instance |
| `base` | empty | per instance (KMS keys, GCP service account) |
| `shared` | single, as today | — |
| `gitlab` | single, as today | — |
| `browser` | single | — |

The alias's main, `base` and `browser` components are deployed by the instance pipelines (see *The `deploy_alias` stage*). `shared` and `gitlab` are deployed manually, as today.

In *alias mode*, the main component owns only the public domain names: the ACM certificates, the API Gateway custom domain names and the Route 53 A records. It doesn't own the base path mappings that connect a public custom domain name to an instance's API. Those are maintained by `scripts/manage_blue_green.py` (see *The state jobs*).

The instances' main components are configured like any other main deployment (see *Configuration*), with `AZUL_SUBDOMAIN_TEMPLATE` set to `*.1` or `*.2`, and the same `AZUL_DOMAIN_NAME` as `anvilprod`. All resource names are qualified with `AZUL_DEPLOYMENT_STAGE`, so the two instances don't collide. These files also exist on `develop`. Which instance a build deploys is not configured in the source (see *Branches and pipelines*).

### `shared` and the ECR images

`shared` remains single. It mirrors upstream images into ECR and, on every regular `apply`, deletes the images that the current commit doesn't use. The two-phase `shared` deploy that already exists (`apply_keep_unused`, then `apply`) covers this. The second phase may run only from a commit that every instance is on, except for resting ones.

The Lambda image repository is unaffected. Its tags are qualified with the stage (`anvilprod1-service`, …), and `--delete-unused` doesn't touch it.


## State

The state is the number of the live instance, `1` or `2`, in an SSM parameter per deployment, such as `/azul/anvilprod/live`. Whether the other instance is standby or resting isn't recorded: it is resting if its OpenSearch domain doesn't exist.

SSM is authoritative. The base path mappings are derived from it, and `scripts/manage_blue_green.py` reconciles them with it, so re-running the `go_live` job repairs a switch that failed halfway (see *The state jobs*).

### Resting and waking

The `rest` job destroys the OpenSearch domain of the instance of the pipeline it runs in, together with its cache-health alarms. It does so with a targeted `terraform destroy` of the same resources that the main-component part of the `_hibernate` convenience function destroys, which is used routinely in tempdev. A targeted destroy also destroys any resources that depend on its targets.

Waking is implicit. Deploying a resting instance recreates whatever the `rest` job destroyed, so pushing to the branch of a resting instance (see *Branches and pipelines*) wakes it. The `deploy` job doesn't read the state, and the Terraform configuration of an instance doesn't depend on it.

### Switching the base path mappings

Switching the base path mapping of an edge-optimized custom domain name from one REST API to another in the same account and region causes no downtime, but takes up to about 80 seconds to propagate (see *How we tested the base path mapping switch*). For about 40 seconds after the call, every request still reaches the old API. For the next 30 to 40 seconds, either API may answer, presumably depending on which CloudFront server handles the request. After that, only the new API answers.

The `go_live` job therefore polls the public names until only the new instance answers, with a timeout of a few minutes. For about half a minute after the job ran, consecutive requests from the same client may be served by different instances.


## Branches and pipelines

Promotion and hotfix PRs target the `anvilprod` branch, as they do today. The `anvilprod` branch is no longer pushed to GitLab. Instead, the operator pushes the merge commit to the GitLab branch named after the instance it is to be deployed to:

```
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

Every pipeline is an instance pipeline. It tests the commit before deploying anything, including the alias components, which matters because hotfixes made during a promotion are committed directly to the `anvilprod` branch and pushed to GitHub and GitLab.

![GitLab pipelines for blue-green deployments](blue-green-pipelines.svg)

| Stage | Jobs |
|---|---|
| `test` | `test` |
| `deploy` | `deploy_base`, `deploy`: the instance |
| `early_reindex` | `early_reindex` (manual) |
| `integration_test` | `integration_test`, against the canonical names |
| `deploy_alias` | `deploy_alias_base`, `deploy_alias`, `deploy_browser`: the alias |
| `teardown` | `on_success`, `on_failure` |
| `reindex` | `reindex` (manual) |
| `state` | `go_live`, `rest` (manual) |

### The `deploy_alias` stage

The `deploy_alias` stage holds the jobs that deploy the alias's components: `deploy_alias_base` deploys `anvilprod.base`, which is empty for now, `deploy_alias` deploys the alias-mode main component `anvilprod`, and `deploy_browser` deploys `anvilprod.browser`. Each job removes the numeric suffix from the name of the deployment being built to select its component. The jobs need names of their own because job names must be unique within a pipeline, and the instance's `deploy_base` and `deploy` jobs already exist.

The stage lies between `integration_test` and `teardown`, so that the alias components are deployed only after the instance passed the integration test. Placing the stage before `teardown` keeps a failure of its jobs reflected in the GitHub status check. The `needs:` of the jobs name `integration_test` rather than the `deploy` job, since `needs:` would otherwise let them start as soon as the `deploy` job finishes, regardless of their stage.

Both instance pipelines deploy the alias components, possibly from different commits, for example when a hotfix is pushed to the previously live instance during the QC period of a blue-green promotion. This is harmless as long as the two commits configure the public domain names the same way, because the base path mappings aren't managed by Terraform.

The Data Browser doesn't inherit Azul's default catalog. Its sites are deployed from the tarballs of the most recent successful Data Browser builds, so deploying the `browser` component from the pipeline of a standby instance doesn't change which catalog the Data Browser defaults to. Making a new catalog the default requires a change to the Data Browser's repository, pushed to GitHub and GitLab. Once the Data Browser build succeeds, the operator retries the `deploy_browser` job in the most recent pipeline of the live instance, which picks up the new tarball. In a blue-green promotion, this must happen after the `go_live` job has run, because only the new instance has the new catalog.

### The state jobs

The `go_live` and `rest` jobs are manual jobs in the `state` stage of every instance pipeline. Both are placed in the GitLab `resource_group: anvilprod`, which makes GitLab run them one at a time.

A single script, `scripts/manage_blue_green.py`, implements both jobs, with one subcommand per job. The `Makefile` has a target for each subcommand, `make go_live` and `make rest`, and each job runs its target, as the `reindex` job runs `make reindex`. Only the script mutates the state and only the script updates the base path mappings. Each subcommand checks its precondition before making any change. The operator can also run the targets from a local checkout, for example when GitLab is unavailable.

The `go_live` subcommand makes the instance of the pipeline it runs in live:

1. Refuse unless the instance's index is complete and its canonical names answer `/health/basic`. The index is complete when every catalog's indices exist and the indexer queues are empty. The job's `needs: [integration_test]` ensures that the integration test passed.

2. Record the instance as live in SSM.

3. Point each public custom domain name's base path mapping at the instance's API and stage.

4. Poll `/version` and `/health/basic` through the public names until only the instance answers, for up to a few minutes (see *Switching the base path mappings*).

Running the `go_live` job again is harmless, and repairs a run that was interrupted after step 2. The subcommand doesn't require the other instance to be on the same commit, because rolling back makes an instance on the previous commit live.

The `rest` subcommand makes the instance of the pipeline it runs in rest. It refuses if the instance is live, and destroys the instance's OpenSearch domain as described in *Resting and waking*. The operator runs the job in the most recent pipeline of the instance, which was built from the commit that last deployed it. The targeted destroy therefore uses the same Terraform configuration and provider versions as the instance's Terraform state. That pipeline may be old, and its job image, tagged with the pipeline's ID, must still be in the GitLab registry. If it isn't, the operator runs `make rest` from a local checkout of that commit instead.


## Integration test

The integration test needs no changes. It runs in the instance's pipeline, with the instance's configuration, so `config.service_endpoint` and `config.indexer_endpoint` resolve to the canonical names, and every AWS resource it touches is qualified with the instance's stage. The OAuth flow in the attended mode of the test redirects to a local HTTP server, so the canonical names don't need to be registered with Google as redirect URIs.

The test doesn't cover the public path, from the public custom domain name through the base path mapping to the instance. The check at the end of the `go_live` job and the QC period cover it.


## Self-referencing URLs

During a request, the service derives its base URL from the `Host` header (`AzulChaliceApp.base_url`). A user who calls the public name receives public URLs, and the integration test, which calls the canonical name, receives canonical ones. This needs no changes.

Outside a request, `base_url` falls back to `config.lambda_endpoint`, which will be the canonical name. The compact and curl manifests use that fallback: they are generated in Step Functions and embed `/repository/files` URLs. Users would receive manifests that refer to the canonical name, and those URLs would break once that instance rests after a later switch.

The fix is to capture the base URL of the request that starts the manifest's Step Functions execution, include it in the `ManifestKey` (and therefore in the manifest hash), and have `generate()` build its URLs from the key rather than from `app.base_url`. Including the base URL in the hash also keeps a manifest generated on a standby instance from being served from the cache after the `go_live` job has run. Taking the value from the key also prevents a race with the `go_live` job between starting the execution and generating the manifest.

Any other code that builds URLs outside of a request would read the SSM parameter (with a short cache) and use the public name when its instance is live and the canonical name otherwise. Whether any such code exists besides the manifest generators hasn't been checked.

URLs that refer to state held by one instance, such as cached manifest objects and manifest fetch tokens, break after a switch no matter which name they use, because each instance has its own storage bucket. How long such URLs need to stay valid limits how soon the old instance may rest.


## Promotions

A promotion switches instances if it requires a full reindex, a catalog-wide reindex, or the addition of a catalog. A catalog-wide reindex affects the performance that all external users observe, and so does adding a catalog. Every other promotion, and every hotfix, is deployed to the live instance in place.

The `reindex:` labels currently can't distinguish a catalog-wide reindex from a partial one: `reindex:partial` covers both. A `reindex:catalog` label or a narrower definition of `reindex:partial` is needed.

A resting instance wakes without any OpenSearch indices, so a switch always indexes every catalog from scratch. Adding `anvil17` to a deployment with `anvil16` means indexing both. That is acceptable for now. Restoring an unchanged catalog's indices from a snapshot of the live domain would be an optimization for promotions that change neither the index schema nor the aggregation logic.

### In-place promotions

Precondition: the other instance is resting. This rules out an in-place promotion during the QC period of a switch.

1. Phase 1 of the `shared` deploy (`apply_keep_unused`)

2. Merge, push `anvilprod:anvilprodL`, where L is the live instance

3. Partial reindex in the `anvilprodL` pipeline, if one is required

4. Phase 2 of the `shared` deploy (`apply`). The resting instance doesn't use the mirrored images, and it will be deployed from the then-current commit when it wakes.

### Blue-green promotions

L is the live instance, S the resting instance that becomes live.

1. Phase 1 of the `shared` deploy (`apply_keep_unused`)

2. Merge, push `anvilprod:anvilprodS`. The `deploy` job wakes S, and the `integration_test` job runs.

3. Run the `early_reindex` or `reindex` job in the `anvilprodS` pipeline

4. Run the `go_live` job in the `anvilprodS` pipeline

5. QC period. L is still on the previous commit with a complete index, so rolling back is a matter of running the `go_live` job in the latest `anvilprodL` pipeline.

6. Run the `rest` job in the latest `anvilprodL` pipeline. L rests on the previous commit. Rolling back after this step requires waking L by pushing to its branch, and reindexing it.

7. Phase 2 of the `shared` deploy (`apply`). Every instance that isn't resting is on the current commit.


## PR templates

The steps are spelled out verbatim, with instance names, in one template per instance and mode:

- `anvilprod1-promotion.md`, `anvilprod2-promotion.md`: in-place promotion of that instance

- `anvilprod1-blue-green-promotion.md`, `anvilprod2-blue-green-promotion.md`: blue-green promotion to that instance

- `anvilprod1-hotfix.md`, `anvilprod2-hotfix.md`

All of them still say "Target branch is `anvilprod`". `scripts/pull_request.py` already derives the template from the name of the promotion branch (`promotions/<date>-<name>` → `<name>-promotion.md`) and the target branch from the item in the template, so branch names such as `promotions/2026-11-21-anvilprod2` and `promotions/2026-11-21-anvilprod2-blue-green` work without changes to the script. The template generator needs reworking: it derives the file name and the target deployments from the target branch (`T.target_branch_by_path`, `T.target_deployments`), which would have to be keyed by instance instead.


## Open questions

1. Other code paths outside of a request that hand out URLs

2. Do the Data Browser or Swagger UI need the canonical names registered as OAuth redirect URIs, for use against a standby instance?

3. Contents and duration of the QC period

4. Migration of the existing `anvilprod` main component into `anvilprod1`, and of its public domain names into the alias-mode component, without downtime

5. `reindex:` label for catalog-wide reindexing


## Appendix

### Why not use DNS alias records

Switching the public names by repointing DNS alias records doesn't work with edge-optimized API Gateway custom domain names. A custom domain name can exist only once, so both instances can't own `service.explore.anvilproject.org`. CloudFront also routes by the `Host` header, so a request for the public name reaching the distribution of `service.1.…` would most likely be rejected. This hasn't been tested, but the existing DRS alias also needed a custom domain name of its own, not just a DNS record.

Switching the base path mapping (`update-base-path-mapping` with patch operations on `/restapiId` and `/stage`) avoids that problem. It doesn't depend on DNS TTLs, takes effect within about 80 seconds (see *Switching the base path mappings*), and switching back is the same operation.

### How we tested the base path mapping switch

The switch was tested on 2026-11-25 in tempdev, by switching the mapping of `service.temp.gi.ucsc.edu` from the service's REST API to the indexer's and back with `update-base-path-mapping`, while two pollers requested `/openapi.json` twice per second, one from a laptop and one from a CloudShell in `us-west-1`, and recorded which app answered:

| | Laptop | CloudShell |
|---|---|---|
| To indexer: first response from the new API | +40 s | +41 s |
| To indexer: last response from the old API | +67 s | +73 s |
| Back to service: first response from the new API | +43 s | +44 s |
| Back to service: last response from the old API | +80 s | +75 s |
| Failed requests | 0 of about 2,000 | 0 of 2,315 |

Both locations saw nearly the same timing.

The pollers must stay below the WAF's rate limit of 1,000 requests per IP in 5 minutes. A first attempt at 10 requests per second per poller was blocked with `429` responses and yielded no data.
