import sys

activate_this = '/var/www/html/dcc-dashboard-service/env/bin/activate_this.py'
execfile(activate_this, dict(__file__=activate_this))
sys.path.insert(0, '/var/www/html/dcc-dashboard-service')

from mapi import app as application
