---
name: Renew server and client VPN certificates 
about: Issue template for the quarterly renewals of VPN certificates
title: Renew VPN server and client certificates 
labels: -,infra,orange
_start: 2025-08-01T09:00
_period: 3 months
---

- [ ] Deploy `tempdev.gitlab`
- [ ] Assign this ticket to the system administrator
- [ ] Renew server certificate on `tempdev`
- [ ] Renew client certificates on `tempdev`
- [ ] Hibernate `tempdev`
- [ ] Renew server certificate on `dev`
- [ ] Renew client certificates on `dev`
- [ ] Renew server certificate on `anvildev`
- [ ] Renew client certificates on `anvildev`
- [ ] Renew server certificate on `prod`
- [ ] Renew client certificates on `prod`
- [ ] Renew server certificate on `anvilprod`
- [ ] Renew client certificates on `anvilprod`
