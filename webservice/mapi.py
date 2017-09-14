from flask import Flask, jsonify, request, session, Blueprint
from billing import billingbp
from action import actionbp
from webservice import webservicebp
import logging

from database import db, login_db, login_manager
from flask_login import LoginManager, login_required, \
    current_user, UserMixin
from flask_sqlalchemy import SQLAlchemy
from flask_migrate import Migrate
from flask.ext.elasticsearch import Elasticsearch
import datetime
import os
from models import Billing, db




logging.basicConfig()


# def create_app(config_obj):
#     app = Flask(__name__)
#     app.config['DEBUG'] = True
#     app.config.from_object(config_obj)
#     app.register_blueprint(actionbp)
#     app.register_blueprint(webservicebp)
#     app.register_blueprint(billingbp)
#     db.init_app(app)
#     login_db.init_app(app)
#     login_manager.init_app(app)
#     apache_path = os.environ.get("APACHE_PATH", "")
#     es_service = os.environ.get("ES_SERVICE", "localhost")
#     login_manager = LoginManager(app)
#     login_manager.login_view = "login"
#     login_manager.session_protection = "strong"
#     migrate = Migrate(app, db)
#     # es = Elasticsearch()
#     es = Elasticsearch(['http://' + es_service + ':9200/'])
#     return app

class Config(object):
    SQLALCHEMY_DATABASE_URI = os.environ.get("DATABASE_URL")
    SQLALCHEMY_BINDS = {
        'login-db': 'postgresql://{}:{}@login-db/{}'.format(os.getenv("L_POSTGRES_USER"),
                                                            os.getenv("L_POSTGRES_PASSWORD"),
                                                            os.getenv("L_POSTGRES_DB"))
    }
    SECRET_KEY = os.environ.get("SECRET_KEY") or "somethingsecret"







""" DB Models """

#class User(login_db.Model, UserMixin):
#    __tablename__ = "users"
#    __bind_key__ = "login-db"
#    id = login_db.Column(login_db.Integer, primary_key=True)
#    email = login_db.Column(login_db.String(100), unique=True, nullable=False)
#    name = login_db.Column(login_db.String(100), nullable=True)
#    avatar = login_db.Column(login_db.String(200))
#    access_token = login_db.Column(login_db.String(5000))
#    redwood_token = login_db.Column(login_db.String(5000))
#    tokens = login_db.Column(login_db.Text)
#    created_at = login_db.Column(login_db.DateTime, default=datetime.datetime.utcnow())

app = Flask(__name__)
app.config['DEBUG'] = True
app.config.from_object(Config)
db.init_app(app)
login_db.init_app(app)
login_manager.init_app(app)
app.register_blueprint(actionbp)
app.register_blueprint(webservicebp)
app.register_blueprint(billingbp)
apache_path = os.environ.get("APACHE_PATH", "")
es_service = os.environ.get("ES_SERVICE", "localhost")
login_manager.login_view = "login"
login_manager.session_protection = "strong"
migrate = Migrate(app, db)
# es = Elasticsearch()
es = Elasticsearch(['http://' + es_service + ':9200/'])
app.app_context()

if __name__ == '__main__':
    app.run()  # Quit the debu and added Threaded
