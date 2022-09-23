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


## Offboarded employee/contractor actions

- [ ] Destroyed all personal deployments that had been created or attempted.
      This includes all cloud resources created for use during the offboarded
      employee/contractor's work on the system, **except** for log resources,
      specifically CloudWatch logs, CloudTrail audit logs and S3 access logs.
- [ ] Assigned ticket to the organization administrator


## System administrator role

- [ ] The offboarded employee/contractor did not perform the
      `system administrator` role

or:

- [ ] An organization administrator has removed the `system administrator` role
      from the offboarded employee/contractor's accounts
- [ ] A system administrator has changed the credentials of all GitLab root
      accounts

Additionally, if none of the remaining team members perform the `system
administrator` role:

- [ ] A project manager nominated [INSERT-NAME-HERE] as a replacement system
      administrator
- [ ] An organization administrator assigned the `system administrator` role
      to the replacement's accounts
- [ ] The offboarded employee/contractor handed over credentials to all GitLab
      root accounts they owned to the replacement


## Organization administrator actions

- [ ] Removed the offboarded employee/contractor's GitHub account from the
      `Azul Developers` team in the DataBiosphere organization on GitHub
- [ ] Removed the offboarded employee/contractor's Google workspace account from
      the Google project used for the deployment
- [ ] Deleted the Google service account (name derived from the Google workspace
      account name) in the Google project used for the deployment

In the `gi-gateway` AWS account:

- [ ] Removed the offboarded employee/contractor's AWS IAM user account from
      the `platform-hca-dev-developer` group
- [ ] Removed the offboarded employee/contractor's AWS IAM user account from
      the `platform-hca-prod-developer` group
- [ ] Removed the offboarded employee/contractor's AWS IAM user account from
      the `platform-anvil-dev-developer` group
- [ ] Removed the offboarded employee/contractor's AWS IAM user account from
      the `platform-anvil-dev-developer` group
- [ ] Removed the offboarded employee/contractor's Google workspace account and
      the Google service account deleted above from the `azul-dev` Terra group

Additionally, if the employee/contractor is leaving UCSC:

- [ ] Removed the offboarded employee/contractor from the `HumanCellAtlas`
      GitHub organization
- [ ] Removed the offboarded employee/contractor from the `DataBiosphere` GitHub
      organization
- [ ] Removed the offboarded employee/contractor from the `ucsc-cgp` GitHub
      organization

Lastly:

- [ ] Assigned ticket to the system administrator


## System administrator actions

- [ ] Removed the offboarded employee/contractor's account from the `Azul
      Operators` team on [GitHub](https://github.com/orgs/DataBiosphere/teams/azul-operators/members)
- [ ] _____ (TODO: Fill in step(s) to remove `GitLab maintainer` role)
- [ ] Removed the offboarded employee/contractor from the `azul-dev` Terra group
- [ ] Removed the offboarded employee/contractor from the `azul-prod` Terra
      group
- [ ] Removed the offboarded employee/contractor from the `azul-anvil-prod`
      Terra group
- [ ] Removed the offboarded employee/contractor from the `Azul Admins` GitHub
      team
- [ ] Removed the offboarded employee/contractor from the `Azul Operators`
      GitHub team
- [ ] Removed the offboarded employee/contractor from the `Azul Managers`
      GitHub team
- [ ] Removed the offboarded employee/contractor from the `Azul Developers`
      GitHub team
- [ ] Removed all role assignments from the offboarded employee/contractor in
      the `dev` instance of GitLab
- [ ] Removed all role assignments from the offboarded employee/contractor in
      the `prod` instance of GitLab
- [ ] Removed all role assignments from the offboarded employee/contractor in
      the `anvildev` instance of GitLab
- [ ] Removed all role assignments from the offboarded employee/contractor in
      the `anvilprod` instance of GitLab
- [ ] Blocked the GitLab account owned by the offboarded employee/contractor on
      every GitLab instance in the system
- [ ] Removed the Google Service accounts owned by the offboarded
      employee/contractor in the `platform-hca-dev` Google Cloud project
- [ ] Removed the Google Service accounts owned by the offboarded
      employee/contractor in the `platform-hca-prod` Google Cloud project
- [ ] Removed the Google Service accounts owned by the offboarded
      employee/contractor in the `platform-anvil-dev` Google Cloud project
- [ ] Removed the Google Service accounts owned by the offboarded
      employee/contractor in the `platform-anvil-prod` Google Cloud project
- [ ] Performed an account review and disabled any accounts owned by the
      offboarded employee/contractor
- [ ] Performed a resource inventory and deleted any resources owned by the
      offboarded employee/contractor
- [ ] Removed the offboarded employee/contractor from the `azul-group` Google
      group
- [ ] Removed any IAM accounts owned by the employee/contractor created for
      AWS CodeCommit
- [ ] Revoked the offboarded employee/contractor's VPN certificate for
      `azul-gitlab-dev`
- [ ] Revoked the offboarded employee/contractor's VPN certificate for
      `azul-gitlab-anvildev`
- [ ] Revoked the offboarded employee/contractor's VPN certificate for
      `azul-gitlab-prod`
- [ ] Revoked the offboarded employee/contractor's VPN certificate for
      `azul-gitlab-anvilprod`
- [ ] Assigned ticket to the project manager


## ITS actions

- [ ] The employee/contractor's employment with the organization is not being
      terminated

or:

- [ ] Disabled the offboarded employee/contractor's UCSC account
- [ ] All shared credentials have been rotated and redistributed to all
      remaining eligible employees and contractors


## Conclusion

- [ ] Close & unassign ticket