Author (before primary review)

- [ ] PR title references issue
- [ ] Title of main commit references issue
- [ ] PR linked to Zenhub issue
- [ ] Added `HCA` label <sub>or this PR does not target an `hca/*` branch</sub>
- [ ] Created issue to track porting these changes <sub>or this PR does not need porting</sub> 
- [ ] Added `[r]` tag to commit title <sub>or this PR does not require reindexing</sub>
- [ ] Added `reindex` label <sub>or this PR does not require reindexing</sub>
- [ ] Freebies are blocked on PR <sub>or there are no freebies in this PR</sub>
- [ ] Freebies are referenced in commit titles <sub>or there are no freebies in this PR</sub>
- [ ] Added `chain` label <sub>or this PR is not the base of another PR</sub>
- [ ] Made this PR a blocker of next PR in chain <sub>or this PR is not the base of another PR</sub>

Primary reviewer (before merging)

- [ ] Checked reindex label and `[r]` tag in commit title
- [ ] Rebased and squashed branch
- [ ] Sanity-checked history
- [ ] Build passes in sandbox <sub>or commented that sandbox will be skipped</sub>
- [ ] Reindexed `sandbox` <sub>or this PR does not require reindexing</sub>

Primary reviewer (after merging)

- [ ] Check `N reviews` labelling is accurate
- [ ] Commented on demo expectations <sub>or label as `no demo`</sub>
- [ ] Reindexed `dev` <sub>or this PR does not require reindexing</sub>
