from flask import Flask
import os
from extensions import sqlalchemy, elasticsearch, migrate
from views import app_bp
import tasks

import logging
logging.basicConfig()

# import json
class Config(object):
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
    add_commands(app)
    return app

def register_extensions(app):
    sqlalchemy.init_app(app)
    migrate.init_app(app, sqlalchemy)
    elasticsearch.init_app(app)

def register_blueprints(app):
    app.register_blueprint(app_bp)

def add_commands(app):
    app.cli.add_command(tasks.generate_daily_reports)
