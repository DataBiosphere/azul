<!-- 
This is the PR template for hotfix PRs against `prod`.
-->

Connected issue: #0000


## Checklist


### Author
- 
- [ ] Target branch is `prod`
- [ ] Source branch is `hotfixes/<github-handle>/<issue#>-<slug>`
- [ ] PR title references the connected issue
- [ ] PR title is `Hotfix: ` followed by title of connected issue
- [ ] PR is connected to issue via Zenhub 
- [ ] PR description links to connected issue


## Author (hotfixes)

- [ ] Added `h` tag to commit title                                 <sub>or this PR does not include a temporary hotfix</sub>
- [ ] Added `H` tag to commit title                                 <sub>or this PR does not include a permanent hotfix</sub>
- [ ] Added `hotfix` label to PR
- [ ] Added `partial` label to PR                                   <sub>or this PR is a permanent hotfix</sub>


### Author (requirements, before every review)

- [ ] Ran `make requirements_update`                                <sub>or this PR does not touch requirements*.txt, common.mk, Makefile and Dockerfile</sub>
- [ ] Added `R` tag to commit title                                 <sub>or this PR does not touch requirements*.txt</sub>
- [ ] Added `reqs` label to PR                                      <sub>or this PR does not touch requirements*.txt</sub>


### Author (before every review)

- [ ] Rebased branch on `prod`, squashed old fixups


### Primary reviewer (after approval)

- [ ] Actually approved the PR
- [ ] Labeled PR `no sandbox`
- [ ] PR title is appropriate as title of merge commit
- [ ] Moved ticket to *Approved* column
- [ ] Assigned PR to current operator


### Operator (before pushing merge the commit)

- [ ] Rebased and squashed branch onto `prod` 
- [ ] Sanity-checked history
- [ ] Pushed PR branch to GitHub
- [ ] Added PR reference to merge commit title
- [ ] Collected commit title tags in merge commit title
- [ ] Moved connected issue to *Merged prod* column in ZenHub
- [ ] Pushed merge commit to GitHub


### Operator (after pushing the merge commit)

- [ ] Pushed merge commit to GitLab                                 <sub>or merge commit can be pushed later, with another PR</sub>
- [ ] Deleted PR branch from GitHub and GitLab
- [ ] Build passes on GitLab


### Operator (reindex) 

- [ ] Started reindex in `prod`                                     <sub>or neither this PR nor a prior failed promotion requires it</sub>
- [ ] Checked for and triaged indexing failures                     <sub>or neither this PR nor a prior failed promotion requires it</sub>
- [ ] Emptied fail queues in target deployment                      <sub>or neither this PR nor a prior failed promotion requires it</sub>


### Operator

- [ ] Unassigned PR


## Shorthand for review comments

- `L` line is too long
- `W` line wrapping is wrong
- `Q` bad quotes
- `F` other formatting problem
