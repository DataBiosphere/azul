Author (before primary review)

- [ ] PR title references issue
- [ ] Title of main commit references issue
- [ ] PR is linked to Zenhub issue
- [ ] Added `DCP/1` label <sub>or this PR does not target an `hca/*` branch</sub>
- [ ] Created issue to track porting these changes <sub>or this PR does not need porting</sub> 
- [ ] Added `[r]` prefix at start of commit title <sub>or this PR does not require reindexing</sub>
- [ ] Added `reindex` label <sub>or this PR does not require reindexing</sub>
- [ ] Freebies are blocked on this PR <sub>or there are no freebies in this PR</sub>
- [ ] Freebies are referenced in commit titles <sub>or there are no freebies in this PR</sub>
- [ ] This PR is blocked by previous PR in chain <sub>or this PR is not chained to another PR</sub>
- [ ] Added `chain` label to the blocking PR <sub>or this PR is not chained to another PR</sub>
- [ ] Ran `make requirements_update` <sub>or this PR does not change requirements*.txt</sub>
- [ ] Mentioned `make requirements` in UPGRADING.rst <sub>or this PR does not change requirements*.txt</sub>
- [ ] Documented upgrade of personal deployments in UPGRADING.rst <sub>or this PR does not require upgrading</sub>

Primary reviewer (before pushing merge commit)

- [ ] Checked `reindex` label and `[r]` prefix of commit title
- [ ] Rebased and squashed branch
- [ ] Sanity-checked history
- [ ] Build passes in `sandbox` <sub>or commented that sandbox will be skipped</sub>
- [ ] Reindexed `sandbox` <sub>or this PR does not require reindexing</sub>
- [ ] Added PR reference to merge commit
- [ ] Moved linked issue to Merged
- [ ] Pushed merge commit to Github

Primary reviewer (after pushing merge commit)

- [ ] Moved freebies to Merged column <sub>or there are no freebies in this PR</sub> 
- [ ] Shortened chain <sub>or this PR is not the base of another PR</sub>
- [ ] Verified that `N reviews` labelling is accurate
- [ ] Commented on demo expectations <sub>or labeled as `no demo`</sub>
- [ ] Pushed merge commit to Gitlab
- [ ] Reindexed `dev` <sub>or this PR does not require reindexing</sub>
- [ ] Deleted PR branch from Github and Gitlab
- [ ] Unassign reviewer from PR
