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

- [ ] Nominated [INSERT-NAME-HERE] as a replacement system administrator <sub>or the system administrator is not being replaced</sub>
- [ ] Assigned ticket to the offboarded employee/contractor


## Offboarded employee/contractor actions

- [ ] Handed over credentials of the GitHub machine account to the replacement system administrator <sub>or the system administrator is not being replaced</sub>
- [ ] Assigned the replacement system administrator's account as a maintainer of the `Azul Triagers` team on GitHub <sub>or the system administrator is not being replaced</sub>
- [ ] Assigned the replacement system administrator's account as a maintainer of the `Azul Developers` team on GitHub <sub>or the system administrator is not being replaced</sub>
- [ ] Assigned the replacement system administrator's account as a maintainer of the `Azul Operators` team on GitHub <sub>or the system administrator is not being replaced</sub>
- [ ] Handed over credentials of all GitLab root accounts to the replacement system administrator <sub>or the system administrator is not being replaced</sub>
- [ ] Handed over local git repository with GitLab configuration to the replacement system administrator <sub>or the system administrator is not being replaced</sub>
- [ ] Added the replacement system administrator's Google workspace account to the `platform-hca-dev` Google project <sub>or the system administrator is not being replaced</sub>
- [ ] Added the replacement system administrator's Google workspace account to the `platform-hca-prod` Google project <sub>or the system administrator is not being replaced</sub>
- [ ] Added the replacement system administrator's Google workspace account to the `platform-anvil-dev` Google project <sub>or the system administrator is not being replaced</sub>
- [ ] Added the replacement system administrator's Google workspace account to the `platform-anvil-prod` Google project <sub>or the system administrator is not being replaced</sub>
- [ ] Added `Project Owner` permissions to the replacement system administrator's Google workspace account <sub>or the system administrator is not being replaced</sub>
- [ ] Added the replacement system administrator's Google workspace account to the `azul-dev` group in Terra <sub>or the system administrator is not being replaced</sub>
- [ ] Added the replacement system administrator's Google workspace account to the `azul-prod` group in Terra <sub>or the system administrator is not being replaced</sub>
- [ ] Added the replacement system administrator's Google workspace account to the `azul-anvil-dev` group in Terra <sub>or the system administrator is not being replaced</sub>
- [ ] Added the replacement system administrator's Google workspace account to the `azul-anvil-prod` group in Terra <sub>or the system administrator is not being replaced</sub>
- [ ] Set the `Can manage users (admin)` checkbox for the replacement system administrator in the `azul-dev` group in Terra <sub>or the system administrator is not being replaced</sub>
- [ ] Set the `Can manage users (admin)` checkbox for the replacement system administrator in the `azul-prod` group in Terra <sub>or the system administrator is not being replaced</sub>
- [ ] Set the `Can manage users (admin)` checkbox for the replacement system administrator in the `azul-anvil-dev` group in Terra <sub>or the system administrator is not being replaced</sub>
- [ ] Set the `Can manage users (admin)` checkbox for the replacement system administrator in the `azul-anvil-prod` group in Terra <sub>or the system administrator is not being replaced</sub>
- [ ] Assigned ticket to the organization administrator


## Organization administrator actions

- [ ] Added the replacement system administrator's AWS IAM user account to the `platform-hca-dev-administrator` IAM group in the `gi-gateway` AWS account <sub>or the system administrator is not being replaced</sub>
- [ ] Added the replacement system administrator's AWS IAM user account to the `platform-hca-prod-administrator` IAM group in the `gi-gateway` AWS account <sub>or the system administrator is not being replaced</sub>
- [ ] Added the replacement system administrator's AWS IAM user account to the `platform-hca-anvildev-administrator` IAM group in the `gi-gateway` AWS account <sub>or the system administrator is not being replaced</sub>
- [ ] Added the replacement system administrator's AWS IAM user account to the `platform-hca-anvilprod-administrator` IAM group in the `gi-gateway` AWS account <sub>or the system administrator is not being replaced</sub>
- [ ] Removed the offboarded employee/contractor's AWS IAM user account from the `platform-hca-dev-viewer` group in the `gi-gateway` AWS account
- [ ] Removed the offboarded employee/contractor's AWS IAM user account from the `platform-hca-dev-developer` group in the `gi-gateway` AWS account
- [ ] Removed the offboarded employee/contractor's AWS IAM user account from the `platform-hca-dev-administrator` group in the `gi-gateway` AWS account
- [ ] Removed the offboarded employee/contractor's AWS IAM user account from the `platform-hca-prod-viewer` group in the `gi-gateway` AWS account
- [ ] Removed the offboarded employee/contractor's AWS IAM user account from the `platform-hca-prod-developer` group in the `gi-gateway` AWS account
- [ ] Removed the offboarded employee/contractor's AWS IAM user account from the `platform-hca-prod-administrator` group in the `gi-gateway` AWS account
- [ ] Removed the offboarded employee/contractor's AWS IAM user account from the `platform-anvil-dev-viewer` group in the `gi-gateway` AWS account
- [ ] Removed the offboarded employee/contractor's AWS IAM user account from the `platform-anvil-dev-developer` group in the `gi-gateway` AWS account
- [ ] Removed the offboarded employee/contractor's AWS IAM user account from the `platform-anvil-dev-administrator` group in the `gi-gateway` AWS account
- [ ] Removed the offboarded employee/contractor's AWS IAM user account from the `platform-anvil-prod-viewer` group in the `gi-gateway` AWS account
- [ ] Removed the offboarded employee/contractor's AWS IAM user account from the `platform-anvil-prod-developer` group in the `gi-gateway` AWS account
- [ ] Removed the offboarded employee/contractor's AWS IAM user account from the `platform-anvil-prod-administrator` group in the `gi-gateway` AWS account
- [ ] Assigned ticket to the system administrator


## System administrator actions

- [ ] Reset the GitHub machine account credentials <sub>or the system administrator is not being replaced</sub>
- [ ] Removed the offboarded employee/contractor from the `HumanCellAtlas` GitHub organization <sub>or the offboarded employee/contractor is not leaving UCSC</sub>
- [ ] Removed the offboarded employee/contractor from the `DataBiosphere` GitHub organization <sub>or the offboarded employee/contractor is not leaving UCSC</sub>
- [ ] Removed the offboarded employee/contractor from the `ucsc-cgp` GitHub organization <sub>or the offboarded employee/contractor is not leaving UCSC</sub>
- [ ] Removed the offboarded employee/contractor from the `Azul Admins` GitHub team
- [ ] Removed the offboarded employee/contractor from the `Azul Developers` GitHub team
- [ ] Removed the offboarded employee/contractor from the `Azul Operators` GitHub team
- [ ] Removed the offboarded employee/contractor from the `Azul Triagers` GitHub team
- [ ] Created a new `dev` GitLab EC2 instance using a private key from the replacement system administrator <sub>or the system administrator is not being replaced</sub>
- [ ] Created a new `prod` GitLab EC2 instance using a private key from the replacement system administrator <sub>or the system administrator is not being replaced</sub>
- [ ] Created a new `anvildev` GitLab EC2 instance using a private key from the replacement system administrator <sub>or the system administrator is not being replaced</sub>
- [ ] Created a new `anvilprod` GitLab EC2 instance using a private key from the replacement system administrator <sub>or the system administrator is not being replaced</sub>
- [ ] Assigned themselves the `GitLab own` role on the `dev` instance of GitLab <sub>or the system administrator is not being replaced</sub>
- [ ] Assigned themselves the `GitLab own` role on the `prod` instance of GitLab <sub>or the system administrator is not being replaced</sub>
- [ ] Assigned themselves the `GitLab own` role on the `anvildev` instance of GitLab <sub>or the system administrator is not being replaced</sub>
- [ ] Assigned themselves the `GitLab own` role on the `anvilprod` instance of GitLab <sub>or the system administrator is not being replaced</sub>
- [ ] Removed the offboarded employee/contractor from the list of members on the Group members page in the `dev` instance of GitLab
- [ ] Removed the offboarded employee/contractor from the list of members on the Group members page in the `prod` instance of GitLab
- [ ] Removed the offboarded employee/contractor from the list of members on the Group members page in the `anvildev` instance of GitLab
- [ ] Removed the offboarded employee/contractor from the list of members on the Group members page in the `anvilprod` instance of GitLab
- [ ] Set the offboarded employee/contractor's account status to `Blocked` on the Users page in the Admin Area of the `dev` instance of GitLab
- [ ] Set the offboarded employee/contractor's account status to `Blocked` on the Users page in the Admin Area of the `prod` instance of GitLab
- [ ] Set the offboarded employee/contractor's account status to `Blocked` on the Users page in the Admin Area of the `anvildev` instance of GitLab
- [ ] Set the offboarded employee/contractor's account status to `Blocked` on the Users page in the Admin Area of the `anvilprod` instance of GitLab
- [ ] Removed all role assignments from the offboarded employee/contractor in the `dev` instance of GitLab
- [ ] Removed all role assignments from the offboarded employee/contractor in the `prod` instance of GitLab
- [ ] Removed all role assignments from the offboarded employee/contractor in the `anvildev` instance of GitLab
- [ ] Removed all role assignments from the offboarded employee/contractor in the `anvilprod` instance of GitLab
- [ ] Removed the offboarded employee/contractor from the `Azul bot account for Gitlab (UCSC Genomics Institute)` group in the `ucsc.edu` Google organization
- [ ] Removed the offboarded employee/contractor's Google workspace account from the `platform-hca-dev` Google project
- [ ] Removed the offboarded employee/contractor's Google workspace account from the `platform-hca-prod` Google project
- [ ] Removed the offboarded employee/contractor's Google workspace account from the `platform-anvil-dev` Google project
- [ ] Removed the offboarded employee/contractor's Google workspace account from the `platform-anvil-prod` Google project
- [ ] Removed the offboarded employee/contractor's Google service account from the `platform-hca-dev` Google project
- [ ] Removed the offboarded employee/contractor's Google service account from the `platform-hca-prod` Google project
- [ ] Removed the offboarded employee/contractor's Google service account from the `platform-anvil-dev` Google project
- [ ] Removed the offboarded employee/contractor's Google service account from the `platform-anvil-prod` Google project
- [ ] Removed the offboarded employee/contractor from the `azul-dev` group in Terra
- [ ] Removed the offboarded employee/contractor from the `azul-prod` group in Terra
- [ ] Removed the offboarded employee/contractor from the `azul-anvil-dev` group in Terra
- [ ] Removed the offboarded employee/contractor from the `azul-anvil-prod` group in Terra
- [ ] Revoked the offboarded employee/contractor's VPN certificate for `dev` and submitted the updated certificate revocation list to AWS Client VPN
- [ ] Revoked the offboarded employee/contractor's VPN certificate for `prod` and submitted the updated certificate revocation list to AWS Client VPN
- [ ] Revoked the offboarded employee/contractor's VPN certificate for `anvildev` and submitted the updated certificate revocation list to AWS Client VPN
- [ ] Revoked the offboarded employee/contractor's VPN certificate for `anvilprod` and submitted the updated certificate revocation list to AWS Client VPN
- [ ] Assigned ticket to the project manager


## ITS actions

- [ ] Disabled the offboarded employee/contractor's UCSC account <sub>or the employee/contractor's employment with the organization is not being terminated</sub>
- [ ] Rotated and redistributed all shared credentials to all remaining eligible employees and contractors <sub>or the employee/contractor's employment with the organization is not being terminated</sub>
- [ ] Assigned ticket to the system administrator


## System administrator actions
 
- [ ] Performed a [resource inventory](https://github.com/DataBiosphere/azul/issues/3634) and deleted any resources owned by the offboarded employee/contractor


## Conclusion

- [ ] Closed & unassigned ticket
