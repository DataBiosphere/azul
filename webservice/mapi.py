from flask import Flask, jsonify, request, session, Blueprint
from billing import billingbp
from action import actionbp
from webservice import webservicebp
import logging

from flask_login import LoginManager, login_required, \
    current_user, UserMixin
from flask_sqlalchemy import SQLAlchemy
from flask_migrate import Migrate
from flask.ext.elasticsearch import Elasticsearch

import os
from models import Billing, db

logging.basicConfig()

app = Flask(__name__)

class Config(object):
    SQLALCHEMY_DATABASE_URI = os.environ.get("DATABASE_URL")
    SQLALCHEMY_BINDS = {
        'login-db': 'postgresql://{}:{}@login-db/{}'.format(os.getenv("L_POSTGRES_USER"),
                                                            os.getenv("L_POSTGRES_PASSWORD"),
                                                            os.getenv("L_POSTGRES_DB"))
    }
    SECRET_KEY = os.environ.get("SECRET_KEY") or "somethingsecret"


apache_path = os.environ.get("APACHE_PATH", "")
es_service = os.environ.get("ES_SERVICE", "localhost")

app = Flask(__name__)
app.config.from_object(Config)
login_db = SQLAlchemy(app)
login_manager = LoginManager(app)
login_manager.login_view = "login"
login_manager.session_protection = "strong"
db.init_app(app)
migrate = Migrate(app, db)
# es = Elasticsearch()
es = Elasticsearch(['http://' + es_service + ':9200/'])

app.register_blueprint(actionbp)
app.register_blueprint(webservicebp)
app.register_blueprint(billingbp)




if __name__ == '__main__':
    app.run()  # Quit the debu and added Threaded
