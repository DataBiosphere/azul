Connected issue: #0000


## Checklist


### Author

- [ ] PR title references the connected issue
- [ ] PR title matches<sup>1</sup> title of connected issue         <sub>or comment in PR explains why they're different</sub>
- [ ] Title of at least one commit references connected issue
- [ ] PR is connected to issue via Zenhub 
- [ ] PR description links to connected issue

<sup>1</sup> when the issue title describes a problem, the PR title is `Fix: ` followed by the issue title   

### Author (reindex)

- [ ] Added `r` tag to commit title                                 <sub>or this PR does not require reindexing</sub>
- [ ] Added `reindex` label to PR                                   <sub>or this PR does not require reindexing</sub>

### Author (chains)

- [ ] This PR is blocked by previous PR in the chain                <sub>or this PR is not chained to another PR</sub>
- [ ] Added `chain` label to the blocking PR                        <sub>or this PR is not chained to another PR</sub>

### Author (upgrading)

- [ ] Documented upgrading of deployments in UPGRADING.rst          <sub>or this PR does not require upgrading</sub>
- [ ] Added `u` tag to commit title                                 <sub>or this PR does not require upgrading</sub>
- [ ] Added `upgrade` label to PR                                   <sub>or this PR does not require upgrading</sub>
- [ ] Added announcement to PR description                          <sub>or this PR does not require announcement</sub>
- [ ] Added checklist items for additional operator tasks           <sub>or this PR does not require additional tasks</sub>

## Author (hotfixes)

- [ ] Added `h` tag to commit title and PR targets `prod`           <sub>or this PR does not include a temporary hotfix</sub>
- [ ] Added `H` tag to commit title                                 <sub>or this PR does not include a permanent hotfix</sub>
- [ ] Added `hotfix` label to PR                                    <sub>or this PR does not include a hotfix</sub>
- [ ] Reverted the temporary hotfix connected to the issue          <sub>or there is no temporary hotfix for the connected issue on the `prod` branch</sub>

### Author (requirements, before every review)

- [ ] Ran `make requirements_update`                                <sub>or this PR does not touch requirements*.txt, common.mk, Makefile and Dockerfile</sub>
- [ ] Added `R` tag to commit title                                 <sub>or this PR does not touch requirements*.txt</sub>
- [ ] Added `reqs` label to PR                                      <sub>or this PR does not touch requirements*.txt</sub>

### Author (before every review)

- [ ] `make integration_test` passes in personal deployment         <sub>or this PR does not touch functionality that could break the IT</sub>
- [ ] Rebased branch on `develop`, squashed old fixups

### Primary reviewer (after approval)

- [ ] Labeled connected issue as `demo` or `no demo`
- [ ] Commented on connected issue about demo expectations          <sub>or labelled connected issue as `no demo`</sub>
- [ ] Decided if PR can be labeled `no sandbox`
- [ ] PR title is appropriate as title of merge commit
- [ ] Moved ticket to Approved column
- [ ] Assigned PR to an operator

### Operator (before pushing merge the commit)

- [ ] Checked `reindex` label and `r` commit title tag
- [ ] Checked that demo expectations are clear                      <sub>or connected issue is labeled as `no demo`</sub>
- [ ] Rebased and squashed branch
- [ ] Sanity-checked history
- [ ] Pushed PR branch to Github
- [ ] Branch pushed to Gitlab and added `sandbox` label             <sub>or PR is labeled `no sandbox`</sub>
- [ ] Build passed in sandbox                                       <sub>or PR is labeled `no sandbox`</sub>
- [ ] Started reindex in `sandbox`                                  <sub>or this PR does not require reindexing `sandbox`</sub>
- [ ] Checked for failures in `sandbox`                             <sub>or this PR does not require reindexing `sandbox`</sub>
- [ ] Added PR reference to merge commit title
- [ ] Collected commit title tags in merge commit title
- [ ] Moved connected issue to Merged column
- [ ] Pushed merge commit to Github

### Operator (after pushing the merge commit)

- [ ] Made announcement requested by author                         <sub>or PR description does not contain an announcement</sub>
- [ ] Shortened the PR chain                                        <sub>or this PR is not the base of another PR</sub>
- [ ] Verified that `N reviews` labelling is accurate               <sub>or this PR is authored by lead</sub>
- [ ] Pushed merge commit to Gitlab                                 <sub>or merge commit can be pushed later, with another PR</sub>
- [ ] Deleted PR branch from Github and Gitlab
- [ ] Build passes on Gitlab
- [ ] Moved connected issue to `prod` or `Merged prod`              <sub>or this PR does not represent a promotion</sub>

### Operator (reindex) 

- [ ] Started reindex in target deployment                          <sub>or this PR does not require reindexing</sub>
- [ ] Checked for and triaged indexing failures                     <sub>or this PR does not require reindexing</sub>
- [ ] Emptied fail queues in target deployment                      <sub>or this PR does not require reindexing</sub>
- [ ] Filed backport PR                                             <sub>or this PR does not represent a hotfix</sub>

### Operator

- [ ] Unassigned PR


## Shorthand for review comments


- `L` line is too long
- `W` line wrapping is wrong
- `Q` bad quotes
- `F` other formatting problem
