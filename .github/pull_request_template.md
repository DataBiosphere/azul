Author

- [ ] PR title references issue
- [ ] Title of main commit references issue
- [ ] PR is linked to Zenhub issue

Author (reindex)

- [ ] Added `r` tag to commit title                         <sub>or this PR does not require reindexing</sub>
- [ ] Added `reindex` label to PR                           <sub>or this PR does not require reindexing</sub>

Author (freebies & chains)

- [ ] Freebies are blocked on this PR                       <sub>or there are no freebies in this PR</sub>
- [ ] Freebies are referenced in commit titles              <sub>or there are no freebies in this PR</sub>
- [ ] This PR is blocked by previous PR in chain            <sub>or this PR is not chained to another PR</sub>
- [ ] Added `chain` label to the blocking PR                <sub>or this PR is not chained to another PR</sub>

Author (upgrading)

- [ ] Documented upgrading of deployments in UPGRADING.rst  <sub>or this PR does not require upgrading</sub>
- [ ] Added `u` tag to commit title                         <sub>or this PR does not require upgrading</sub>
- [ ] Added `upgrade` label to PR                           <sub>or this PR does not require upgrading</sub>

Author (requirements, before every review)

- [ ] Ran `make requirements_update`                        <sub>or this PR leaves requirements*.txt, common.mk and Makefile untouched</sub>
- [ ] Added `R` tag to commit title                         <sub>or this PR leaves requirements*.txt untouched</sub>
- [ ] Added `reqs` label to PR                              <sub>or this PR leaves requirements*.txt untouched</sub>

Author (before every review)

- [ ] `make integration_test` passes in personal deployment <sub>or this PR does not touch functionality that could break the IT</sub>
- [ ] Rebased branch on `develop`, squashed old fixups

Primary reviewer (before pushing merge commit)

- [ ] Checked `reindex` label and `r` commit title tag
- [ ] Rebased and squashed branch
- [ ] Sanity-checked history
- [ ] Build passes in `sandbox`                             <sub>or added `no sandbox` label</sub>
- [ ] Reindexed `sandbox`                                   <sub>or this PR does not require reindexing `sandbox`</sub>
- [ ] Added PR reference to merge commit
- [ ] Moved linked issue to Merged
- [ ] Pushed merge commit to Github

Primary reviewer (after pushing merge commit)

- [ ] Moved freebies to Merged column                       <sub>or there are no freebies in this PR</sub> 
- [ ] Shortened chain                                       <sub>or this PR is not the base of another PR</sub>
- [ ] Verified that `N reviews` labelling is accurate
- [ ] Commented on demo expectations                        <sub>or labeled as `no demo`</sub>
- [ ] Pushed merge commit to Gitlab                         <sub>or this changes can be pushed later, together with another PR</sub>
- [ ] Deleted PR branch from Github and Gitlab

Primary reviewer (reindex) 

- [ ] Started reindex in `dev`                              <sub>or this PR does not require reindexing `dev`</sub>
- [ ] Deleted BQ slot committment in `dev`                  <sub>or this PR does not require reindexing `dev`</sub>
- [ ] Checked for failures in `dev`                         <sub>or this PR does not require reindexing `dev`</sub>
- [ ] Started reindex in `prod`                             <sub>or this PR does not require reindexing `prod`</sub>
- [ ] Deleted BQ slot committment in `prod`                 <sub>or this PR does not require reindexing `prod`</sub>
- [ ] Checked for failures in `prod`                        <sub>or this PR does not require reindexing `prod`</sub>

Primary reviewer

- [ ] Unassign PR from reviewer and assign to PM
