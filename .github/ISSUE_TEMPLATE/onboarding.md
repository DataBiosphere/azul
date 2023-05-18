---
name: Onboarding
about: 'Template for onboarding new developers'
title: Onboarding to Azul
labels: infra, task
assignees: ''
---

# Onboarding

This ticket should be created by the hiring supervisor to initiate the process
of onboarding an employee to team Boardwalk. The action items in this ticket
will be completed by the supervisor, other team members, and the onboarded
employee. The ticket should first be assigned to the hiring supervisor.

If you are assigned this ticket, perform the checklist items in order. Remove
your assignment only after assigning this ticket to the next in line or the
ticket checklist is completed. At that time, put the ticket back in triage.


## Hiring Supervisor 

- [ ] Onboarded employee's name: [ENTER-NAME-HERE]
- [ ] Approved the HR request for ITS to create a UCSC account for the onboarded employee
- [ ] Instructed the onboarded employee to create a new GitHub account using the username `{ucsc_account_name}-ucsc` connected to their UCSC account email address. The new account will be used exclusively for work on the team and should be created even if they already have another account.
- [ ] Assigned this ticket to the onboarded employee

## Onboarded employee actions

- [ ] Onboarded employee's UCSC account email: [ENTER-EMAIL-HERE]
- [ ] Onboarded employee's GitHub account username: [ENTER-USERNAME-HERE]
- [ ] Read [Azul README.md](..)
- [ ] Read [Contributing Guidelines](../blob/develop/CONTRIBUTING.rst)
- [ ] Read [AWS Credential Configuration](https://giwiki.gi.ucsc.edu/index.php/Overview_of_Getting_and_Using_an_AWS_IAM_Account)
- [ ] Completed the NIH [Information Security and Management Refresher](https://irtsectraining.nih.gov/publicUser.aspx) training courses
- [ ] Completed the NIH [Information Security Awareness for New Hires](https://irtsectraining.nih.gov/publicUser.aspx) training courses
- [ ] Completed the NIH [Information Management for New Hires](https://irtsectraining.nih.gov/publicUser.aspx) training courses
- [ ] Completed the [dbGaP Code of Conduct](https://www.surveymonkey.com/r/HKXNYD7) quiz
- [ ] Completed the [NIH Security Best Practices for (CAD) Subject to NIH (GDS) Policy](https://www.surveymonkey.com/r/FG3C63T) quiz
- [ ] Digitally signed the [NIH GDS Policy](https://giwiki.gi.ucsc.edu/images/1/1b/NIH_GDS_Policy.pdf) document and gave it to the hiring supervisor
- [ ] Installed [git-secrets](..#211-git-secrets)
- [ ] Completed steps for [setting up a VPN client](..#911-setting-up-a-vpn-client) for `dev.gitlab` and `anvildev.gitlab`
- [ ] Sent a request email to the organization administrator (cc: hiring supervisor & system administrator) asking them to:
    - Create an AWS IAM user account
    - Assign the `developer` role in the dev accounts (`platform-{hca,anvil}-dev`) to the AWS IAM user account
    - Assign the `developer` role to the onboarded employee's GitHub user account
    - Add the onboarded employee's UCSC account to the `firecloud-cgl` group in Terra
    - Send confirmation to all email recipients when the request has been completed
- [ ] Assigned this ticket to the hiring supervisor

## Hiring supervisor actions

- [ ] Approved (by replying to) the request email sent by the onboarded employee to the organization administrator
- [ ] Received confirmation from the organization administrator that the request has been completed
- [ ] Assigned this ticket to the onboarded employee

## Onboarded employee actions

- [ ] Sent a request email to the system administrator (cc: hiring supervisor) asking them to:
    - Create a GitLab user account on the dev instances of GitLab
    - Assign the `developer` role to the account
- [ ] Assigned this ticket to the hiring supervisor

## Hiring supervisor actions

- [ ] Approved (by replying to) the request email sent by the onboarded employee to the system administrator
- [ ] Assigned this ticket to the system administrator

## System administrator actions

- [ ] Verified the authenticity of the approval
- [ ] Verified the GitHub user account is associated with the onboarded employee's UCSC account email
- [ ] Created a GitLab user account for the onboarded employee on the `dev` instance of GitLab
- [ ] Created a GitLab user account for the onboarded employee on the `anvildev` instance of GitLab
- [ ] Added the onboarded employee to exactly one of the groups on the `dev` instance of GitLab and set the `Max Role` to `Developer`
- [ ] Added the onboarded employee to exactly one of the groups on the `anvildev` instance of GitLab and set the `Max Role` to `developer`
- [ ] Associated the new GitLab user accounts with the onboarded employee's GitHub user account
- [ ] Added the onboarded employee to the `Azul Developers` GitHub team
- [ ] Added the onboarded employee's UCSC account to the `platform-hca-dev` Google project and assigned the `developer` role
- [ ] Added the onboarded employee's UCSC account to the `platform-anvil-dev` Google project and assigned the `developer` role
- [ ] Added the onboarded employee's Google Workspace account to the `platform-hca-dev` Google project and assigned `Project Editor` permissions
- [ ] Added the onboarded employee's Google Workspace account to the `platform-anvil-dev` Google project and assigned `Project Editor` permissions
- [ ] Assigned this ticket to the hiring supervisor

## Hiring supervisor actions

- [ ] Added the onboarded employee to the [UCSC-CGL Slack](https://ucsc-cgl.slack.com)
- [ ] Assigned this ticket to the system administrator

## System administrator actions

- [ ] Gave the onboarded employee push access to the Azul Repository on [#team-boardwalk](https://ucsc-cgl.slack.com/archives/C705Y6G9Z)
- [ ] Gave the onboarded developer access to development CI/CD systems on [#team-boardwalk](https://ucsc-cgl.slack.com/archives/C705Y6G9Z)
- [ ] Gave the onboarded employee [GCP Credentials](https://console.cloud.google.com/)
- [ ] Added the onboarded employee's Google Workspace account to the `azul-dev` group in Terra
- [ ] Added the onboarded employee's Google Workspace account to the `azul-anvil-dev` group in Terra
- [ ] Assigned this ticket to the hiring supervisor

## Hiring supervisor actions (after probationary period)

- [ ] Sent a request email to the organization administrator (cc: onboarded employee, hiring supervisor & system administrator) asking them to:
    - Assign the `viewer` role in the prod accounts (`platform-{hca,anvil}-prod`) to the AWS IAM user account
    - Send confirmation to all email recipients when the request has been completed
- [ ] Sent a request email to the system administrator (cc: onboarded employee & hiring supervisor) asking them to:
    - Create a GitLab user account on the production instances of GitLab
    - Associate the GitLab user account with the employee's GitHub user account
    - Assign the `developer` role to the GitLab user account
    - Add the employee's UCSC account to the Google Cloud project in the prod deployment zone
    - Send confirmation to all email recipients when the request has been completed
- [ ] Received confirmation from the organization administrator that the request has been completed
- [ ] Received confirmation from the system administrator that the request has been completed
- [ ] Assigned this ticket to the system administrator

## System administrator actions (after probationary period)

- [ ] Added the onboarded employee's Google Workspace account to the `azul-prod` group in Terra
- [ ] Added the onboarded employee's Google Workspace account to the `azul-anvil-prod` group in Terra
- [ ] Added the onboarded employee to the operator role rotation in Slack <sub>or the onboarded employee will not be performing the operator role</sub>

## Conclusion

- [ ] Closed & unassigned this ticket
