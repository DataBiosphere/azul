<!-- 
This is the PR template for backport PRs against `develop`.
-->

## Checklist


### Author

- [ ] Target branch is `develop`
- [ ] Source branch is `backports/<7-digit SHA1 of most recent backported commit>`
- [ ] PR title contains the 7-digit SHA1 of the backported commits
- [ ] PR title references the issues relating to the backported commits
- [ ] PR title references the PRs that introduced the backported commits


### Author (requirements, before every review)

- [ ] Ran `make requirements_update`                                <sub>or this PR does not touch requirements*.txt, common.mk, Makefile and Dockerfile</sub>
- [ ] Added `R` tag to commit title                                 <sub>or this PR does not touch requirements*.txt</sub>
- [ ] Added `reqs` label to PR                                      <sub>or this PR does not touch requirements*.txt</sub>


### Primary reviewer (after rejection)

Uncheck the *Author (requirements)* and *Author (rebasing, integration test)* 
checklists.


### Primary reviewer (after approval)

- [ ] Actually approved the PR
- [ ] Decided if PR can be labeled `no sandbox`
- [ ] PR title is appropriate as title of merge commit
- [ ] Moved ticket to *Approved* column
- [ ] Assigned PR to current operator


### Operator (before pushing merge the commit)

- [ ] Sanity-checked history
- [ ] Pushed PR branch to GitHub
- [ ] Branch pushed to GitLab and added `sandbox` label             <sub>or PR is labeled `no sandbox`</sub>
- [ ] Build passed in sandbox                                       <sub>or PR is labeled `no sandbox`</sub>
- [ ] Added PR reference (this PR) to merge commit title
- [ ] Collected commit title tags in merge commit title
- [ ] Pushed merge commit to GitHub


### Operator (after pushing the merge commit)

- [ ] Pushed merge commit to GitLab                                 <sub>or merge commit can be pushed later, with another PR</sub>
- [ ] Deleted PR branch from GitHub and GitLab
- [ ] Build passes on GitLab


### Operator

- [ ] Unassigned PR


## Shorthand for review comments

- `L` line is too long
- `W` line wrapping is wrong
- `Q` bad quotes
- `F` other formatting problem
