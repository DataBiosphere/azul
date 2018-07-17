#!/bin/bash
rm -rf cgp-dashboard-service
chalice new-project cgp-dashboard-service
rm -f cgp-dashboard-service/app.py cgp-dashboard-service/requirements.txt 
rsync -a src/ cgp-dashboard-service/vendor
cp lambdas/service/app.py cgp-dashboard-service
cd cgp-dashboard-service
chalice local --port=9000
