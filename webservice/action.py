from flask import Flask, jsonify, request, session, Blueprint
from flask_login import LoginManager, login_required, \
     current_user, UserMixin
# import json
# from flask_sqlalchemy import SQLAlchemy
from flask_cors import CORS, cross_origin
from flask import redirect
# from flask_migrate import Migrate
# import flask_excel as excel
from flask.ext.elasticsearch import Elasticsearch
# import ast
# from decimal import Decimal
# import copy

import os
# from models import Billing, db
# from utility import get_compute_costs, get_storage_costs, create_analysis_costs_json, create_storage_costs_json
import datetime
# import calendar
# import click
# TEST database call
import logging
from database import db, login_db, login_manager, User
from monitordb_lib import luigiDBInit
from sqlalchemy import select, desc

actionbp = Blueprint('actionbp', 'actionbp')

logging.basicConfig()

es_service = os.environ.get("ES_SERVICE", "localhost")
es = Elasticsearch(['http://' + es_service + ':9200/'])

@login_manager.user_loader
def load_user(user_id):
    return User.query.get(int(user_id))

@actionbp.route('/login')
def login():
    if current_user.is_authenticated:
        redirect('https://{}'.format(os.getenv('DCC_DASHBOARD_HOST')))
    else:
        redirect('https://{}/login'.format(os.getenv('DCC_DASHBOARD_HOST')))

@actionbp.route('/action/service')
@login_required
@cross_origin()
def get_action_service():
    monitordb_connection, monitordb_table, db_engine = luigiDBInit()
    select_query = select([monitordb_table]).order_by(desc("last_updated"))
    select_result = monitordb_connection.execute(select_query)
    result_list = jsonify([dict(row) for row in select_result])
    monitordb_connection.close()
    db_engine.dispose()
    return result_list

@actionbp.route('/')
@cross_origin()
def hello():
    return 'Hello World!'

