---
name: A New Issue
about: 'Template for new issues'
title: 
labels: orange 
assignees: ''
---

<!--
Add your description here. For suspected bugs, the steps required to reproduce
the problem, as well as the expected and actual outcome should be included in
the description. Solutions should be proposed in a  separate comment, not the
issue description. Please do not modify or delete the checklist below. The
checklist should remain at the end of the issue description.
-->

- [ ] Security design review completed; the Resolution of this issue does **not** …
  - [ ] … affect authentication; for example:
    - OAuth 2.0 with the application (API or Swagger UI)
    - Authentication of developers with Google Cloud APIs
    - Authentication of developers with AWS APIs
    - Authentication with a GitLab instance in the system
    - Password and 2FA authentication with GitHub
    - API access token authentication with GitHub
    - Authentication with 
  - [ ] … affect the permissions of internal users like access to
    - Cloud resources on AWS and GCP
    - GitLab repositories, projects and groups, administration
    - an EC2 instance via SSH
    - GitHub issues, pull requests, commits, commit statuses, wikis, repositories, organizations
  - [ ] … affect the permissions of external users like access to
    - TDR snapshots
  - [ ] … affect permissions of service or bot accounts
    - Cloud resources on AWS and GCP
  - [ ] … affect audit logging in the system, like
    - adding, removing or changing a log message that represents an auditable event
    - changing the routing of log messages through the system
  - [ ] … affect monitoring of the system
  - [ ] … introduce a new software dependency like
    - Python packages on PYPI
    - Command-line utilities
    - Docker images
    - Terraform providers
  - [ ] … add an interface that exposes sensitive or confidential data at the security boundary
  - [ ] … affect the encryption of data at rest
  - [ ] … require persistence of sensitive or confidential data that might require encryption at rest
  - [ ] … require unencrypted transmission of data within the security boundary
  - [ ] … affect the network security layer; for example by 
    - modifying, adding or removing firewall rules
    - modifying, adding or removing security groups
    - changing or adding a port a service, proxy or load balancer listens on
- [ ] Documentation on any unchecked boxes is provided in comments below
