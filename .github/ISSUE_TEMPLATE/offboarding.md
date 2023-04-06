---
name: Offboarding
about: 'Template for offboarding employees'
title: Offboard [INSERT-NAME-HERE]
labels: infra, task
assignees: ''
---

# Offboarding

By creating this ticket, you are requesting an employee to be offboarded from 
team Boardwalk. The action items in this ticket will be completed by the 
project manager, other team members, and the offboarded employee. The ticket
should first be assigned to the offboarded employee.

If you are assigned this ticket, perform the checklist items in order. Remove
your assignment only after assigning this ticket to the next in line or the
ticket checklist is completed. At that time, put the ticket back in triage.


## Offboarded employee actions

- [ ] Offboarded employee's name: [INSERT-NAME-HERE]
- [ ] Offboarded employee's UCSC account: [INSERT-ACCOUNT-HERE]
- [ ] Offboarded employee's GitHub handle: [INSERT-HANDLE-HERE]
- [ ] Offboarded employee's AWS IAM username: [INSERT-IAM-USERNAME-HERE]
- [ ] Employee is leaving UCSC? [ANSWER-YES-OR-NO]
- [ ] Destroyed all personal deployments that had been created or attempted. This includes all cloud resources created for use during the offboarded employee's work on the system, **except** for log resources, specifically CloudWatch logs, CloudTrail audit logs and S3 access logs
- [ ] Assigned ticket to the project manager


## Project manager actions

- [ ] Nominated [INSERT-NAME-HERE] as a replacement system administrator <sub>or the system administrator is not being replaced</sub>
- [ ] Assigned ticket to the offboarded employee


## Offboarded employee actions

- [ ] Handed over credentials of the GitHub machine account to the replacement system administrator <sub>or the system administrator is not being replaced</sub>
- [ ] Assigned the replacement system administrator's account as a maintainer of the `Azul Triagers` team on GitHub <sub>or the system administrator is not being replaced</sub>
- [ ] Assigned the replacement system administrator's account as a maintainer of the `Azul Developers` team on GitHub <sub>or the system administrator is not being replaced</sub>
- [ ] Assigned the replacement system administrator's account as a maintainer of the `Azul Operators` team on GitHub <sub>or the system administrator is not being replaced</sub>
- [ ] Handed over credentials of all GitLab root accounts to the replacement system administrator <sub>or the system administrator is not being replaced</sub>
- [ ] Handed over local git repository with GitLab configuration to the replacement system administrator <sub>or the system administrator is not being replaced</sub>
- [ ] Added the replacement system administrator's Google Workspace account to the `platform-hca-dev` Google project <sub>or the system administrator is not being replaced</sub>
- [ ] Added the replacement system administrator's Google Workspace account to the `platform-hca-prod` Google project <sub>or the system administrator is not being replaced</sub>
- [ ] Added the replacement system administrator's Google Workspace account to the `platform-anvil-dev` Google project <sub>or the system administrator is not being replaced</sub>
- [ ] Added the replacement system administrator's Google Workspace account to the `platform-anvil-prod` Google project <sub>or the system administrator is not being replaced</sub>
- [ ] Added `Project Owner` permissions to the replacement system administrator's Google Workspace account <sub>or the system administrator is not being replaced</sub>
- [ ] Added the replacement system administrator's Google Workspace account to the `azul-dev` group in Terra <sub>or the system administrator is not being replaced</sub>
- [ ] Added the replacement system administrator's Google Workspace account to the `azul-prod` group in Terra <sub>or the system administrator is not being replaced</sub>
- [ ] Added the replacement system administrator's Google Workspace account to the `azul-anvil-dev` group in Terra <sub>or the system administrator is not being replaced</sub>
- [ ] Added the replacement system administrator's Google Workspace account to the `azul-anvil-prod` group in Terra <sub>or the system administrator is not being replaced</sub>
- [ ] Set the `Can manage users (admin)` checkbox for the replacement system administrator in the `azul-dev` group in Terra <sub>or the system administrator is not being replaced</sub>
- [ ] Set the `Can manage users (admin)` checkbox for the replacement system administrator in the `azul-prod` group in Terra <sub>or the system administrator is not being replaced</sub>
- [ ] Set the `Can manage users (admin)` checkbox for the replacement system administrator in the `azul-anvil-dev` group in Terra <sub>or the system administrator is not being replaced</sub>
- [ ] Set the `Can manage users (admin)` checkbox for the replacement system administrator in the `azul-anvil-prod` group in Terra <sub>or the system administrator is not being replaced</sub>
- [ ] Sent a request email to the organization administrator (cc: project manager & system administrator) asking them to: 
    - Remove the offboarded employee's AWS IAM user account from the `platform-{hca,anvil}-{dev,prod}-{viewer,developer,administrator}` groups in the `gi-gateway` AWS account
    - If the system administrator is being replaced, add the replacement system administrator's AWS IAM user account to the `platform-hca-{dev,prod,anvildev,anvilprod}-administrator` IAM groups in the `gi-gateway` AWS account
    - Send confirmation to all email recipients when the request has been completed
- [ ] Assigned this ticket to the project manager

## Project manager actions

- [ ] Approved (by replying to) the request email sent by the offboarded employee to the organization administrator
- [ ] Received confirmation from the organization administrator that the request has been completed
- [ ] Assigned ticket to the system administrator


## System administrator actions

- [ ] Reset the GitHub machine account credentials <sub>or the system administrator is not being replaced</sub>
- [ ] Removed the offboarded employee from the `HumanCellAtlas` GitHub organization <sub>or the offboarded employee is not leaving UCSC</sub>
- [ ] Removed the offboarded employee from the `DataBiosphere` GitHub organization <sub>or the offboarded employee is not leaving UCSC</sub>
- [ ] Removed the offboarded employee from the `ucsc-cgp` GitHub organization <sub>or the offboarded employee is not leaving UCSC</sub>
- [ ] Removed the offboarded employee from the `Azul Admins` GitHub team
- [ ] Removed the offboarded employee from the `Azul Developers` GitHub team
- [ ] Removed the offboarded employee from the `Azul Operators` GitHub team
- [ ] Removed the offboarded employee from the `Azul Triagers` GitHub team
- [ ] Created a new `dev` GitLab EC2 instance using a private key from the replacement system administrator <sub>or the system administrator is not being replaced</sub>
- [ ] Created a new `prod` GitLab EC2 instance using a private key from the replacement system administrator <sub>or the system administrator is not being replaced</sub>
- [ ] Created a new `anvildev` GitLab EC2 instance using a private key from the replacement system administrator <sub>or the system administrator is not being replaced</sub>
- [ ] Created a new `anvilprod` GitLab EC2 instance using a private key from the replacement system administrator <sub>or the system administrator is not being replaced</sub>
- [ ] Assigned themselves the `GitLab own` role on the `dev` instance of GitLab <sub>or the system administrator is not being replaced</sub>
- [ ] Assigned themselves the `GitLab own` role on the `prod` instance of GitLab <sub>or the system administrator is not being replaced</sub>
- [ ] Assigned themselves the `GitLab own` role on the `anvildev` instance of GitLab <sub>or the system administrator is not being replaced</sub>
- [ ] Assigned themselves the `GitLab own` role on the `anvilprod` instance of GitLab <sub>or the system administrator is not being replaced</sub>
- [ ] Removed the offboarded employee from the list of members on the Group members page in the `dev` instance of GitLab
- [ ] Removed the offboarded employee from the list of members on the Group members page in the `prod` instance of GitLab
- [ ] Removed the offboarded employee from the list of members on the Group members page in the `anvildev` instance of GitLab
- [ ] Removed the offboarded employee from the list of members on the Group members page in the `anvilprod` instance of GitLab
- [ ] Set the offboarded employee's account status to `Blocked` on the Users page in the Admin Area of the `dev` instance of GitLab
- [ ] Set the offboarded employee's account status to `Blocked` on the Users page in the Admin Area of the `prod` instance of GitLab
- [ ] Set the offboarded employee's account status to `Blocked` on the Users page in the Admin Area of the `anvildev` instance of GitLab
- [ ] Set the offboarded employee's account status to `Blocked` on the Users page in the Admin Area of the `anvilprod` instance of GitLab
- [ ] Removed all role assignments from the offboarded employee in the `dev` instance of GitLab
- [ ] Removed all role assignments from the offboarded employee in the `prod` instance of GitLab
- [ ] Removed all role assignments from the offboarded employee in the `anvildev` instance of GitLab
- [ ] Removed all role assignments from the offboarded employee in the `anvilprod` instance of GitLab
- [ ] Removed the offboarded employee from the `Azul bot account for Gitlab (UCSC Genomics Institute)` group in the `ucsc.edu` Google organization
- [ ] Removed the offboarded employee's Google Workspace account from the `platform-hca-dev` Google project
- [ ] Removed the offboarded employee's Google Workspace account from the `platform-hca-prod` Google project
- [ ] Removed the offboarded employee's Google Workspace account from the `platform-anvil-dev` Google project
- [ ] Removed the offboarded employee's Google Workspace account from the `platform-anvil-prod` Google project
- [ ] Removed the offboarded employee's Google service account from the `platform-hca-dev` Google project
- [ ] Removed the offboarded employee's Google service account from the `platform-hca-prod` Google project
- [ ] Removed the offboarded employee's Google service account from the `platform-anvil-dev` Google project
- [ ] Removed the offboarded employee's Google service account from the `platform-anvil-prod` Google project
- [ ] Removed the offboarded employee from the `azul-dev` group in Terra
- [ ] Removed the offboarded employee from the `azul-prod` group in Terra
- [ ] Removed the offboarded employee from the `azul-anvil-dev` group in Terra
- [ ] Removed the offboarded employee from the `azul-anvil-prod` group in Terra
- [ ] Revoked the offboarded employee's VPN certificate for `dev` and submitted the updated certificate revocation list to AWS Client VPN
- [ ] Revoked the offboarded employee's VPN certificate for `prod` and submitted the updated certificate revocation list to AWS Client VPN
- [ ] Revoked the offboarded employee's VPN certificate for `anvildev` and submitted the updated certificate revocation list to AWS Client VPN
- [ ] Revoked the offboarded employee's VPN certificate for `anvilprod` and submitted the updated certificate revocation list to AWS Client VPN
- [ ] Assigned ticket to the project manager


## Project manager

- [ ] If the offboarded employee is leaving UCSC, sent a request email to ITS (cc: system administrator) asking them to:
    - Disable the offboarded employee's UCSC account
    - Rotate and redistribute all shared credentials to all remaining eligible employees
- [ ] Received confirmation from ITS that the request has been completed <sub>or the offboarded employee is not leaving UCSC</sub>
- [ ] Assigned ticket to the system administrator


## System administrator actions

- [ ] Performed an account review and disabled any accounts owned by the offboarded employee 
- [ ] Performed a [resource inventory](https://github.com/DataBiosphere/azul/issues/3634) and deleted any resources owned by the offboarded employee


## Conclusion

- [ ] Closed & unassigned ticket
