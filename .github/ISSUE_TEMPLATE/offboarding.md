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
- [ ] Assigned ticket to the organization administrator.

## System administrator role

- [ ] The offboarded employee/contractor did not perform the
      `system administrator` role.

or:

- [ ] An organization administrator has removed the `system administrator` role
      from the offboarded employee/contractor's accounts.
- [ ] A system administrator has changed the credentials of all GitLab root
      accounts.

Additionally, if none of the remaining team members perform the `system
administrator` role:

- [ ] A project manager nominated [INSERT-NAME-HERE] as a replacement system
      administrator.
- [ ] An organization administrator assigned the `system administrator` role
      to the replacement's accounts.
- [ ] The offboarded employee/contractor handed over credentials to all GitLab
      root accounts they owned to the replacement.


## Organization administrator actions

- [ ] Removed the `Azul modify` role (a component of the `operator` role) from
      the offboarded employee/contractor for all deployments and all zones.
- [ ] Removed the employee/contractor from the UCSC GitHub organization, or the
      employee/contractor's employment with the organization has not been
      terminated.
- [ ] Assigned ticket to the system administrator.


## System administrator actions

- [ ] Removed the remaining components of the `operator` role from the
      offboarded employee/contractor.
- [ ] Removed all remaining roles assigned to the offboarded employee/contractor.
- [ ] Blocked the GitLab account owned by the offboarded employee/contractor on
      every GitLab instance in the system.
- [ ] Removed the Google Service accounts owned by the offboarded
      employee/contractor on every Google Cloud project in the system.
- [ ] Performed an account review and disabled any accounts owned by the
      offboarded employee/contractor.
- [ ] Performed a resource inventory and deleted any resources owned by the
      offboarded employee/contractor.
- [ ] Removed the offboarded employee/contractor from the `azul-group` Google
      group.
- [ ] Removed any IAM accounts owned by the employee/contractor created for
      AWS CodeCommit.
- [ ] Assigned ticket to the project manager.


## ITS actions

- [ ] The employee/contractor's employment with the organization is not being
      terminated.

or:

- [ ] Disabled the offboarded employee/contractor's UCSC account.
- [ ] All shared credentials have been rotated and redistributed to all
      remaining eligible employees and contractors.
- [ ] Close & unassign ticket.