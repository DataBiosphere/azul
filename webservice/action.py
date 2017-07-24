from flask import Flask, jsonify, request, session, Blueprint
from flask_login import LoginManager, login_required, \
     current_user, UserMixin
# import json
# from flask_sqlalchemy import SQLAlchemy
from flask_cors import CORS, cross_origin
# from flask_migrate import Migrate
# import flask_excel as excel
# from flask.ext.elasticsearch import Elasticsearch
# import ast
# from decimal import Decimal
# import copy

import os
# from models import Billing, db
# from utility import get_compute_costs, get_storage_costs, create_analysis_costs_json, create_storage_costs_json
# import datetime
# import calendar
# import click
# TEST database call
from sqlalchemy import create_engine, MetaData, String, Table, Float, Column, select
import logging

actionbp = Blueprint('actionbp', 'actionbp', url_prefix='/actionbp')

logging.basicConfig()

@actionbp.route('/action/service')
@login_required
@cross_origin()
def get_action_service():
    db = create_engine(
        'postgresql://{}:{}@db/monitor'.format(os.getenv("POSTGRES_USER"), os.getenv("POSTGRES_PASSWORD")), echo=False)
    conn = db.connect()
    metadata = MetaData(db)
    luigi = Table('luigi', metadata,
                  Column("luigi_job", String(100), primary_key=True),
                  Column("status", String(20)),
                  Column("submitter_specimen_id", String(100)),
                  Column("specimen_uuid", String(100)),
                  Column("workflow_name", String(100)),
                  Column("center_name", String(100)),
                  Column("submitter_donor_id", String(100)),
                  Column("consonance_job_uuid", String(100)),
                  Column("submitter_donor_primary_site", String(100)),
                  Column("project", String(100)),
                  Column("analysis_type", String(100)),
                  Column("program", String(100)),
                  Column("donor_uuid", String(100)),
                  Column("submitter_sample_id", String(100)),
                  Column("submitter_experimental_design", String(100)),
                  Column("submitter_specimen_type", String(100)),
                  Column("workflow_version", String(100)),
                  Column("sample_uuid", String(100)),
                  Column("start_time", String(100)),
                  Column("last_updated", String(100))
                  )
    select_query = select([luigi]).order_by("last_updated")
    select_result = conn.execute(select_query)
    result_list = [dict(row) for row in select_result]
    return jsonify(result_list)

@actionbp.route('/action/service/hello')
@login_required
@cross_origin()
def hello():
    return 'Hello World!'

