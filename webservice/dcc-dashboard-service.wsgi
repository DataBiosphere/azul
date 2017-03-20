import sys
import os

activate_this = '/var/www/html/dcc-dashboard-service/env/bin/activate_this.py'
execfile(activate_this, dict(__file__=activate_this))
sys.path.insert(0, '/var/www/html/dcc-dashboard-service')

def application(req_environ, start_response):

    ENV_VAR =[
       'DATABASE_URL',
       'APACHE_PATH'
    ]
    for key in ENV_VAR:
       os.environ[key] = req_environ.get(key, '')
    ## has to import my app inside of application def block.
    from mapi import app as _application

    return _application(req_environ, start_response)
