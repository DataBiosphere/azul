Author (before primary review)

- [ ] PR title references issue
- [ ] Title of main commit references issue
- [ ] PR is linked to Zenhub issue
- [ ] Added `HCA` label <sub>or this PR does not target an `hca/*` branch</sub>
- [ ] Created issue to track porting these changes <sub>or this PR does not need porting</sub> 
- [ ] Added `[r]` prefix at start of commit title <sub>or this PR does not require reindexing</sub>
- [ ] Added `reindex` label <sub>or this PR does not require reindexing</sub>
- [ ] Freebies are blocked on this PR <sub>or there are no freebies in this PR</sub>
- [ ] Freebies are referenced in commit titles <sub>or there are no freebies in this PR</sub>
- [ ] Added `chain` label <sub>or this PR is not the base of another PR</sub>
- [ ] Made this PR a blocker of next PR in chain <sub>or this PR is not the base of another PR</sub>
- [ ] Ran `make update_requirements` <sub>or this PR does not change requirements*.txt</sub>
- [ ] Mentioned `make requirements` in UPGRADING.rst <sub>or this PR does not change requirements*.txt</sub>
- [ ] Documented upgrade of personal deployments in UPGRADING.rst <sub>or this PR does not require upgrading</sub>

Primary reviewer (before pushing merge commit)

- [ ] Checked `reindex` label and `[r]` prefix of commit title
- [ ] Rebased and squashed branch
- [ ] Sanity-checked history
- [ ] Build passes in `sandbox` <sub>or commented that sandbox will be skipped</sub>
- [ ] Reindexed `sandbox` <sub>or this PR does not require reindexing</sub>
- [ ] Added PR reference to merge commit

Primary reviewer (after pushing merge commit)

- [ ] Pushed merge commit to Github and Gitlab
- [ ] Shortened chain <sub>or this PR is not the base of another PR</sub>
- [ ] Deleted PR branch from Github and Gitlab
- [ ] Verified that `N reviews` labelling is accurate
- [ ] Commented on demo expectations <sub>or labeled as `no demo`</sub>
- [ ] Reindexed `dev` <sub>or this PR does not require reindexing</sub>
