---
name: Offboarding
about: 'Template for offboarding employees/contractors'
title: Offboard [INSERT-NAME-HERE]
labels: infra, task
assignees: ''
---

# Offboarding

By creating this ticket, you are requesting a employee/contractor to be
offboarded from the team Boardwalk. The action items in this ticket should be
carried out by the offboarded employee/contractor and existing team members.
Your next step is to assign this ticket to the offboarding employee/contractor.

If you are assigned this ticket, perform the checklist items in order. Unassign
yourself only after you have assigned the ticket to the next in line, or the
ticket has been completed. At that time, put the ticket back to triage.


### Employee/contractor's details

- [ ] Name: [INSERT-NAME-HERE]
- [ ] UCSC account: [INSERT-ACCOUNT-HERE]
- [ ] GitHub handle: [INSERT-HANDLE-HERE]
- [ ] AWS IAM user name: [INSERT-IAM-USER-NAME-HERE]
- [ ] Employee/contractor is leaving UCSC? [ANSWER-YES-OR-NO]


## Offboarded employee/contractor actions

- [ ] Destroyed all personal deployments that had been created or attempted. This includes all cloud resources created for use during the offboarded employee/contractor's work on the system, **except** for log resources, specifically CloudWatch logs, CloudTrail audit logs and S3 access logs
- [ ] Assigned ticket to the project manager


## Project manager actions

- [ ] Nominated [INSERT-NAME-HERE] as a replacement system administrator <sub>or the offboarded employee/contractor was not the sole system administrator</sub>
- [ ] Assigned ticket to the offboarded employee/contractor


## Offboarded employee/contractor actions

- [ ] Handed over credentials of all GitLab root accounts to the replacement system administrator <sub>or the offboarded employee/contractor was not the sole system administrator</sub>
- [ ] Assigned ticket to the organization administrator


## Organization administrator actions

- [ ] Removed the `system administrator` role from the offboarded employee/contractor's accounts <sub>or the offboarded employee/contractor did not perform the `system administrator` role</sub>
- [ ] Assigned the `system administrator` role to the replacement nominated by the project manager <sub>or the offboarded employee/contractor was not the sole `system administrator`</sub> 
- [ ] Removed the offboarded employee/contractor's GitHub account from the `Azul Admins` team in the DataBiosphere organization on GitHub
- [ ] Removed the offboarded employee/contractor's Google workspace account from all Google projects used in any deployment zones
- [ ] Deleted the Google service account (name derived from the Google workspace account name) in all Google projects used in any deployment zones
- [ ] Removed the offboarded employee/contractor's Google workspace account and the Google service account deleted above from the `azul-dev` group in Terra
- [ ] Removed the offboarded employee/contractor from the `HumanCellAtlas` GitHub organization <sub>or the offboarded employee/contractor is not leaving UCSC</sub>
- [ ] Removed the offboarded employee/contractor from the `DataBiosphere` GitHub organization <sub>or the offboarded employee/contractor is not leaving UCSC</sub>
- [ ] Removed the offboarded employee/contractor from the `ucsc-cgp` GitHub organization <sub>or the offboarded employee/contractor is not leaving UCSC</sub>
- [ ] Removed the offboarded employee/contractor's AWS IAM user account from the `platform-hca-dev-viewer` group in the `gi-gateway` AWS account
- [ ] Removed the offboarded employee/contractor's AWS IAM user account from the `platform-hca-dev-developer` group in the `gi-gateway` AWS account
- [ ] Removed the offboarded employee/contractor's AWS IAM user account from the `platform-hca-prod-viewer` group in the `gi-gateway` AWS account
- [ ] Removed the offboarded employee/contractor's AWS IAM user account from the `platform-hca-prod-developer` group in the `gi-gateway` AWS account
- [ ] Removed the offboarded employee/contractor's AWS IAM user account from the `platform-anvil-dev-viewer` group in the `gi-gateway` AWS account
- [ ] Removed the offboarded employee/contractor's AWS IAM user account from the `platform-anvil-dev-developer` group in the `gi-gateway` AWS account
- [ ] Removed the offboarded employee/contractor's AWS IAM user account from the `platform-anvil-prod-viewer` group in the `gi-gateway` AWS account
- [ ] Removed the offboarded employee/contractor's AWS IAM user account from the `platform-anvil-prod-developer` group in the `gi-gateway` AWS account
- [ ] Assigned ticket to the system administrator


## System administrator actions

- [ ] Changed the credentials of all GitLab root accounts <sub>or the offboarded employee/contractor did not perform the `system administrator` role</sub>
- [ ] Removed the offboarded employee/contractor's account from the `Azul Operators` team on [GitHub](https://github.com/orgs/DataBiosphere/teams/azul-operators/members)
- [ ] Removed the `GitLab maintainer` role from the offboarded employee/contractor (TODO: Fill in details of steps required to do this)
- [ ] Removed the offboarded employee/contractor from the `azul-dev` group in Terra
- [ ] Removed the offboarded employee/contractor from the `azul-prod` group in Terra
- [ ] Removed the offboarded employee/contractor from the `azul-anvil-prod` group in Terra
- [ ] Removed the offboarded employee/contractor from the `Azul Admins` GitHub team
- [ ] Removed the offboarded employee/contractor from the `Azul Operators` GitHub team
- [ ] Removed the offboarded employee/contractor from the `Azul Triagers` GitHub team
- [ ] Removed the offboarded employee/contractor from the `Azul Developers` GitHub team
- [ ] Removed all role assignments from the offboarded employee/contractor in the `dev` instance of GitLab
- [ ] Removed all role assignments from the offboarded employee/contractor in the `prod` instance of GitLab
- [ ] Removed all role assignments from the offboarded employee/contractor in the `anvildev` instance of GitLab
- [ ] Removed all role assignments from the offboarded employee/contractor in the `anvilprod` instance of GitLab
- [ ] Blocked the GitLab account owned by the offboarded employee/contractor on every GitLab instance in the system
- [ ] Removed the Google Service accounts owned by the offboarded employee/contractor in the `platform-hca-dev` Google Cloud project
- [ ] Removed the Google Service accounts owned by the offboarded employee/contractor in the `platform-hca-prod` Google Cloud project
- [ ] Removed the Google Service accounts owned by the offboarded employee/contractor in the `platform-anvil-dev` Google Cloud project
- [ ] Removed the Google Service accounts owned by the offboarded employee/contractor in the `platform-anvil-prod` Google Cloud project
- [ ] Performed an account review and disabled any accounts owned by the offboarded employee/contractor
- [ ] Performed a resource inventory and deleted any resources owned by the offboarded employee/contractor
- [ ] Removed the offboarded employee/contractor from the `azul-group` group in Google
- [ ] Removed any IAM accounts owned by the employee/contractor created for AWS CodeCommit
- [ ] Revoked the offboarded employee/contractor's VPN certificate for `dev`
- [ ] Revoked the offboarded employee/contractor's VPN certificate for `anvildev`
- [ ] Revoked the offboarded employee/contractor's VPN certificate for `prod`
- [ ] Revoked the offboarded employee/contractor's VPN certificate for `anvilprod`
- [ ] Assigned ticket to the project manager


## ITS actions

- [ ] Disabled the offboarded employee/contractor's UCSC account <sub>or the employee/contractor's employment with the organization is not being terminated</sub>
- [ ] Rotated and redistributed all shared credentials to all remaining eligible employees and contractors <sub>or the employee/contractor's employment with the organization is not being terminated</sub>


## Conclusion

- [ ] Close & unassign ticket
