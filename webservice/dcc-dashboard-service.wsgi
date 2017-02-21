import sys
import os

activate_this = '/var/www/html/dcc-dashboard-service/env/bin/activate_this.py'
execfile(activate_this, dict(__file__=activate_this))
sys.path.insert(0, '/var/www/html/dcc-dashboard-service')

from app import app as application

def application(req_environ, start_response):

    ENV_VAR =[
       'GOOGLE_CLIENT_ID',
       'GOOGLE_CLIENT_SECRET',
       'REDWOOD_ADMIN',
       'REDWOOD_ADMIN_PASSWORD',
       'REDWOOD_SERVER',
       'REDWOOD_ADMIN_PORT',
       'DCC_DASHBOARD_HOST',
       'DCC_DASHBOARD_PORT',
       'DCC_DASHBOARD_PROTOCOL',
       'DCC_DASHBOARD_SERVICE',
       'DATABASE_URL'
    ]
    for key in ENV_VAR:
       os.environ[key] = req_environ.get(key, '')
    ## has to import my app inside of application def block.
    from mapi import app as _application

    return _application(req_environ, start_response)
