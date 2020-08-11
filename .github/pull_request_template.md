---
name: default
about: 'A PR with check lists for author and primary reviewer'
title: 
labels: orange 
assignees: ''
---

Author (before primary review)

- [ ] PR title references issue
- [ ] Title of main commit references issue
- [ ] PR linked to Zenhub issue
- [ ] Added `HCA` label <font color="#888">or this PR does not target an `hca/*` branch</font>
- [ ] Created issue to track porting these changes <font color="#888">or this PR does not need porting</font> 
- [ ] Added `[r]` tag to commit title <font color="#888">or this PR does not require reindexing</font>
- [ ] Added `reindex` label <font color="#888">or this PR does not require reindexing</font>
- [ ] Freebies are blocked on PR <font color="#888">or there are no freebies in this PR</font>
- [ ] Freebies are referenced in commit titles <font color="#888">or there are no freebies in this PR</font>
- [ ] Added `chain` label <font color="#888">or this PR is not the base of another PR</font>
- [ ] Made this PR a blocker of next PR in chain <font color="#888">or this PR is not the base of another PR</font>

Primary reviewer (before merging)

- [ ] Checked reindex label and `[r]` tag in commit title
- [ ] Rebased and squashed branch
- [ ] Sanity-checked history
- [ ] Build passes in sandbox <font color="#888">or commented that sandbox will be skipped</font>
- [ ] Reindexed `sandbox` <font color="#888">or this PR does not require reindexing</font>

Primary reviewer (after merging)

- [ ] Check `N reviews` labelling is accurate
- [ ] Commented on demo expectations <font color="#888">or label as `no demo`</font>
- [ ] Reindexed `dev` <font color="#888">or this PR does not require reindexing</font>
