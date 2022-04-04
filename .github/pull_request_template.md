<!-- 
This is the PR template for regular PRs against `develop`. Edit the URL in your 
browser's location bar, appending either `&template=promotion.md`, 
`&template=hotfix.md` or `&template=backport.md` to switch the template.    
-->

Connected issue: #0000


## Checklist


### Author

- [ ] Target branch is `develop`
- [ ] Source branch is `issues/<github-handle>/<issue#>-<slug>`
- [ ] PR title references the connected issue
- [ ] PR title matches<sup>1</sup> title of connected issue         <sub>or comment in PR explains why they're different</sub>
- [ ] Title of at least one commit references connected issue
- [ ] PR is connected to issue via Zenhub 
- [ ] PR description links to connected issue
- [ ] Added `partial` label to PR                                    <sub>or this PR completely resolves the connected issue</sub>

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


### Author (operator tasks)

- [ ] Added checklist items for additional operator tasks           <sub>or this PR does not require additional tasks</sub>


### Author (hotfixes)

- [ ] Added `F` tag to main commit title                            <sub>or this PR does not include permanent fix for a temporary hotfix</sub>
- [ ] Reverted the temporary hotfix connected to the issue          <sub>or there is no temporary hotfix for the connected issue on the `prod` branch</sub>


### Author (requirements)

- [ ] Ran `make requirements_update`                                <sub>or this PR does not touch requirements*.txt, common.mk, Makefile and Dockerfile</sub>
- [ ] Added `R` tag to commit title                                 <sub>or this PR does not touch requirements*.txt</sub>
- [ ] Added `reqs` label to PR                                      <sub>or this PR does not touch requirements*.txt</sub>


### Author (rebasing, integration test)

- [ ] `make integration_test` passes in personal deployment         <sub>or this PR does not touch functionality that could break the IT</sub>
- [ ] Rebased branch on `develop`, squashed old fixups


### Primary reviewer (after rejection)

Uncheck the *Author (requirements)* and *Author (rebasing, integration test)* 
checklists.


### Primary reviewer (after approval)

- [ ] Actually approved the PR
- [ ] Labeled connected issue as `demo` or `no demo`
- [ ] Commented on connected issue about demo expectations          <sub>or labelled connected issue as `no demo`</sub>
- [ ] Decided if PR can be labeled `no sandbox`
- [ ] PR title is appropriate as title of merge commit
- [ ] Ticket is in *Review requested* column
- [ ] Requested review from peer reviewer
- [ ] Assigned PR to peer reviewer


### Peer reviewer (after rejection)

Uncheck the *Author (requirements)* and *Author (rebasing, integration test)* 
checklists.


### Peer reviewer (after approval)

- [ ] `N reviews` label on PR reflects prior peer reviews with changes requested
- [ ] Moved ticket to *Approved* column
- [ ] Assigned PR to current operator


### Operator (before pushing merge the commit)

- [ ] Checked `reindex` label and `r` commit title tag
- [ ] Checked that demo expectations are clear                      <sub>or connected issue is labeled as `no demo`</sub>
- [ ] Rebased and squashed branch
- [ ] Sanity-checked history
- [ ] Pushed PR branch to GitHub
- [ ] Branch pushed to GitLab and added `sandbox` label             <sub>or PR is labeled `no sandbox`</sub>
- [ ] Build passed in sandbox                                       <sub>or PR is labeled `no sandbox`</sub>
- [ ] Deleted unreferenced indices in `sandbox`                     <sub>or this PR does not remove catalogs or otherwise causes unreferenced indices</sub> 
- [ ] Started reindex in `sandbox`                                  <sub>or this PR does not require reindexing `sandbox`</sub>
- [ ] Checked for failures in `sandbox`                             <sub>or this PR does not require reindexing `sandbox`</sub>
- [ ] Added PR reference to merge commit title
- [ ] Collected commit title tags in merge commit title
- [ ] Moved connected issue to Merged column
- [ ] Pushed merge commit to GitHub


### Operator (after pushing the merge commit)

- [ ] Shortened the PR chain                                        <sub>or this PR is not labeled `chain`</sub>
- [ ] Pushed merge commit to GitLab                                 <sub>or merge commit can be pushed later, with another PR</sub>
- [ ] Deleted PR branch from GitHub and GitLab
- [ ] Build passes on GitLab


### Operator (reindex) 

- [ ] Deleted unreferenced indices in `dev`                         <sub>or this PR does not remove catalogs or otherwise causes unreferenced indices</sub> 
- [ ] Started reindex in `dev`                                      <sub>or this PR does not require reindexing</sub>
- [ ] Checked for and triaged indexing failures                     <sub>or this PR does not require reindexing</sub>
- [ ] Emptied fail queues in target deployment                      <sub>or this PR does not require reindexing</sub>


### Operator

- [ ] Unassigned PR


## Shorthand for review comments

- `L` line is too long
- `W` line wrapping is wrong
- `Q` bad quotes
- `F` other formatting problem
