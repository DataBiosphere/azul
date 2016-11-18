
from flask import Flask
# import the FlaskElasticsearch package
import os
from extensions import sqlalchemy, elasticsearch, scheduler, migrate
from views import app_bp
from tasks import *

import logging
logging.basicConfig()

# import json
class Config(object):
    JOBS = [
        # Runs once a day at 12:02 AM, is responsible for daily
        # integrating budgets and updating them
        {'id': 'closeMonthlyBills',
         'func': '{}:close_out_billings'.format(__name__),
         'trigger': 'cron',
         'args': (),
         'minute': '2',
         # run this 2 minutes later than updateDailyBilling so we close things out correctly
         'hour': '0',
         'day': '1'},

        # Runs once a month at midnight UTC on the first day of the month
        {'id': 'updateDailyBilling',
         'func': '{}:generate_daily_reports'.format(__name__),
         'trigger': 'cron',
         'args': (),
         'minute': '0',
         'hour': '0',
         }
    ]
    SQLALCHEMY_DATABASE_URI = os.environ.get("DATABASE_URL")


# start scheduler, this is an alternative to using celery, supports cron
def create_app(config_object=Config):
    """An application factory, as explained here: http://flask.pocoo.org/docs/patterns/appfactories/.

    :param config_object: The configuration object to use.
    """
    app = Flask(__name__)

    app.config.from_object(config_object)
    register_extensions(app)
    register_blueprints(app)
    return app

def register_extensions(app):
    sqlalchemy.init_app(app)
    scheduler.init_app(app)
    scheduler.start()
    migrate.init_app(app, sqlalchemy)
    elasticsearch.init_app(app)

def register_blueprints(app):
    app.register_blueprint(app_bp)
