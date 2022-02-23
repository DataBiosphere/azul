https://github.com/DataBiosphere/azul/issues/NUMBER


## Checklist


### Author

- [ ] PR title references issue
- [ ] PR title matches issue title (preceded by `Fix: ` for bugs)   <sub>or there is a good reason why they're different</sub>
- [ ] Title of main commit references issue
- [ ] PR is connected to Zenhub issue and description links to issue

### Author (reindex)

- [ ] Added `r` tag to commit title                         <sub>or this PR does not require reindexing</sub>
- [ ] Added `reindex` label to PR                           <sub>or this PR does not require reindexing</sub>

### Author (freebies & chains)

- [ ] Freebies are blocked on this PR                       <sub>or there are no freebies in this PR</sub>
- [ ] Freebies are referenced in commit titles              <sub>or there are no freebies in this PR</sub>
- [ ] This PR is blocked by previous PR in the chain        <sub>or this PR is not chained to another PR</sub>
- [ ] Added `chain` label to the blocking PR                <sub>or this PR is not chained to another PR</sub>

### Author (upgrading)

- [ ] Documented upgrading of deployments in UPGRADING.rst  <sub>or this PR does not require upgrading</sub>
- [ ] Added `u` tag to commit title                         <sub>or this PR does not require upgrading</sub>
- [ ] Added `upgrade` label to PR                           <sub>or this PR does not require upgrading</sub>
- [ ] Added announcement to PR description                  <sub>or this PR does not require announcement</sub>
- [ ] Added checklist items for additional operator tasks   <sub>or this PR does not require additional tasks</sub>

### Author (requirements, before every review)

- [ ] Ran `make requirements_update`                        <sub>or this PR does not touch requirements*.txt, common.mk, Makefile and Dockerfile</sub>
- [ ] Added `R` tag to commit title                         <sub>or this PR does not touch requirements*.txt</sub>
- [ ] Added `reqs` label to PR                              <sub>or this PR does not touch requirements*.txt</sub>

### Author (before every review)

- [ ] `make integration_test` passes in personal deployment <sub>or this PR does not touch functionality that could break the IT</sub>
- [ ] Rebased branch on `develop`, squashed old fixups

### Primary reviewer (after approval)

- [ ] Commented in issue about demo expectations            <sub>or labelled issue as `no demo`</sub>
- [ ] Decided if PR can be labeled `no sandbox`
- [ ] PR title is appropriate as title of merge commit
- [ ] Moved ticket to Approved column
- [ ] Assigned PR to an operator

### Operator (before pushing merge the commit)

- [ ] Checked `reindex` label and `r` commit title tag
- [ ] Checked that demo expectations are clear              <sub>or issue is labeled as `no demo`</sub>
- [ ] Rebased and squashed branch
- [ ] Sanity-checked history
- [ ] Pushed PR branch to Github
- [ ] Branch pushed to Gitlab and added `sandbox` label     <sub>or PR is labeled `no sandbox`</sub>
- [ ] Build passed in sandbox                               <sub>or PR is labeled `no sandbox`</sub>
- [ ] Started reindex in `sandbox`                          <sub>or this PR does not require reindexing `sandbox`</sub>
- [ ] Checked for failures in `sandbox`                     <sub>or this PR does not require reindexing `sandbox`</sub>
- [ ] Added PR reference to merge commit title
- [ ] Collected commit title tags in merge commit title
- [ ] Moved linked issue to Merged column
- [ ] Pushed merge commit to Github

### Operator (after pushing the merge commit)

- [ ] Made announcement requested by author                 <sub>or PR description does not contain an announcement</sub>
- [ ] Moved freebies to Merged column                       <sub>or there are no freebies in this PR</sub> 
- [ ] Shortened the PR chain                                <sub>or this PR is not the base of another PR</sub>
- [ ] Verified that `N reviews` labelling is accurate       <sub>or this PR is authored by lead</sub>
- [ ] Pushed merge commit to Gitlab                         <sub>or merge commit can be pushed later, with another PR</sub>
- [ ] Deleted PR branch from Github and Gitlab
- [ ] Build passes on Gitlab
- [ ] Moved issues to `prod` or `Merged prod`               <sub>or this PR does not represent a promotion</sub>

### Operator (reindex) 

- [ ] Started reindex in target deployment                  <sub>or this PR does not require reindexing</sub>
- [ ] Checked for and triaged indexing failures             <sub>or this PR does not require reindexing</sub>
- [ ] Emptied fail queues in target deployment              <sub>or this PR does not require reindexing</sub>
- [ ] Filed backport PR                                     <sub>or this PR does not represent a hotfix</sub>

### Operator

- [ ] Unassigned PR


## Shorthand for review comments


- `L` line is too long
- `W` line wrapping is wrong
- `Q` bad quotes
- `F` other formatting issue
