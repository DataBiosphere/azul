# from flask import jsonify, Blueprint
# from flask_login import login_required, \
#      current_user
# from flask import redirect
# from flask.ext.elasticsearch import Elasticsearch
# import os
# import logging
# from chalice import Chalice
# from database import login_manager, User
# from monitordb_lib import luigiDBInit
# from sqlalchemy import select, desc
# actionbp = Chalice('actionbp')
#
#
# actionbp = Blueprint('actionbp', 'actionbp')
#
# logging.basicConfig()
#
# es_service = os.environ.get("ES_SERVICE", "localhost")
# es = Elasticsearch(['http://' + es_service + ':9200/'])
#
#
# @login_manager.user_loader
# def load_user(user_id):
#     return User.query.get(int(user_id))
#
#
# @actionbp.route('/login')
# def login():
#     if current_user.is_authenticated:
#         redirect('https://{}'.format(os.getenv('DCC_DASHBOARD_HOST')))
#     else:
#         redirect('https://{}/login'.format(os.getenv('DCC_DASHBOARD_HOST')))
#
#
# @actionbp.route('/action/service')
# @login_required
# def get_action_service():
#     monitordb_connection, monitordb_table, db_engine = luigiDBInit()
#     select_query = select([monitordb_table]).order_by(desc("last_updated"))
#     select_result = monitordb_connection.execute(select_query)
#     result_list = jsonify([dict(row) for row in select_result])
#     monitordb_connection.close()
#     db_engine.dispose()
#     return result_list
#
#
# @actionbp.route('/')
# def hello():
#     return 'Hello World!'
