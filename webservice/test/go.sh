#!/bin/bash
rm -rf cgp-dashboard-service
chalice new-project cgp-dashboard-service
rm -f cgp-dashboard-service/app.py cgp-dashboard-service/requirements.txt 
cp -a chalicelib cgp-dashboard-service/ 
cp app.py cgp-dashboard-service 
cd cgp-dashboard-service
chalice local --port=9000
