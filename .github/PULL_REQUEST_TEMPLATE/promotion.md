<!-- 
This is the PR template for promotion PRs against `prod`.
-->

Connected issue: #0000


## Checklist


### Author

- [ ] Target branch is `prod`
- [ ] Source branch is `promotions/yyyy-mm-dd` 
- [ ] PR title matches issue title (`Promotion yyyy-mm-dd`)
- [ ] PR is connected to issue via Zenhub 
- [ ] PR description links to connected issue

### Author (reindex)

- [ ] Added `reindex` label to PR                                   <sub>or this promotion does not require reindexing</sub>


### Author (upgrading)

- [ ] Added `upgrade` label to PR                                   <sub>or this promotion does not require upgrading</sub>


### Primary reviewer (after approval)

- [ ] Actually approved the PR
- [ ] Labeled PR as `no sandbox`
- [ ] Moved ticket to *Approved* column
- [ ] Assigned PR to current operator


### Operator (before pushing merge the commit)

- [ ] Pushed PR branch to GitHub
- [ ] Added PR reference to merge commit title
- [ ] Collected commit title tags in merge commit title
- [ ] Pushed merge commit to GitHub


### Operator (after pushing the merge commit)

- [ ] Made announcement requested by author                         <sub>or PR description does not contain an announcement</sub>
- [ ] Shortened the PR chain                                        <sub>or this PR is not the base of another PR</sub>
- [ ] Pushed merge commit to GitLab                                 <sub>or merge commit can be pushed later, with another PR</sub>
- [ ] Deleted PR branch from GitHub and GitLab
- [ ] Build passes on GitLab
- [ ] Moved connected issue to *Merged prod* column on ZenHub
- [ ] Moved promoted issues from *Merged* to *Merged prod* column  <sub>or this PR does not represent a promotion</sub>
- [ ] Moved promoted issues from *dev* to *prod* column            <sub>or this PR does not represent a promotion</sub>


### Operator (reindex) 

- [ ] Started reindex in `prod`                                     <sub>or this PR does not require reindexing</sub>
- [ ] Checked for and triaged indexing failures                     <sub>or this PR does not require reindexing</sub>
- [ ] Emptied fail queues in target deployment                      <sub>or this PR does not require reindexing</sub>


### Operator

- [ ] Unassigned PR


## Shorthand for review comments

- `L` line is too long
- `W` line wrapping is wrong
- `Q` bad quotes
- `F` other formatting problem
