from flask import Flask, jsonify, request, session, Blueprint
from flask_login import LoginManager, login_required, \
    current_user, UserMixin
# import json
# from flask_sqlalchemy import SQLAlchemy
from flask_cors import CORS, cross_origin
from flask_migrate import Migrate
# import flask_excel as excel
from flask.ext.elasticsearch import Elasticsearch
from flask import redirect
# import ast
from decimal import Decimal
# import copy

import os
from models import Billing, db
from utility import get_compute_costs, get_storage_costs, create_analysis_costs_json, create_storage_costs_json
import datetime
import calendar
import click
# TEST database call
# from sqlalchemy import create_engine, MetaData, String, Table, Float, Column, select
import logging
from database import db, login_db, login_manager, User

billingbp = Blueprint('billingbp', 'billingbp')

logging.basicConfig()

es_service = os.environ.get("ES_SERVICE", "localhost")
es = Elasticsearch(['http://' + es_service + ':9200/'])

@login_manager.user_loader
def load_user(user_id):
    return User.query.get(int(user_id))

@billingbp.route('/login')
def login():
    if current_user.is_authenticated:
        redirect('https://{}'.format(os.getenv('DCC_DASHBOARD_HOST')))
    else:
        redirect('https://{}/login'.format(os.getenv('DCC_DASHBOARD_HOST')))

@billingbp.route('/invoices')
@login_required
@cross_origin()
def find_invoices():
    project = str(request.args.get('project'))
    if project:
        invoices = [invoice.to_json() for invoice in Billing.query.filter(Billing.project == project).order_by(
            Billing.end_date.desc()).all()]
        return jsonify(invoices)
    else:
        return None, 401


@billingbp.route('/projects')
@cross_origin()
def get_projects():
    es_resp = es.search(index='billing_idx', body={"query": {"match_all": {}}, "aggs": {
        "projects": {
            "terms": {
                "field": "project.keyword",
                "size": 9999
            }
        }
    }}, size=0)

    projects = []
    for project in es_resp['aggregations']['projects']['buckets']:
        projects.append(project['key'])
    return jsonify(projects)


def get_projects_list():

    es_resp = es.search(index='billing_idx', body={"query": {"match_all": {}}, "aggs": {
        "projects": {
            "terms": {
                "field": "project.keyword",
                "size": 9999
            }
        }
    }}, size=0)

    projects = []
    for project in es_resp['aggregations']['projects']['buckets']:
        projects.append(project['key'])
    return projects


def make_search_filter_query(timefrom, timetil, project):
    """

    :param timefrom: datetime object, filters all values less than this
    :param timetil: datetime object, filters all values greater than or equal to this
    :param project: string, this is the name of the particular project that we are trying to generate for
    :return:
    """

    timestartstring = timefrom.strftime('%Y-%m-%dT%H:%M:%S')
    timeendstring = timetil.strftime('%Y-%m-%dT%H:%M:%S')
    es_resp = es.search(index='billing_idx', body={
        "query": {
            "bool": {
                "must": [
                    {
                        "term": {
                            "project.keyword": project
                        }
                    },
                    {
                        "nested": {
                            "path": "specimen.samples.analysis",
                            "score_mode": "max",
                            "query": {
                                "range": {
                                    "specimen.samples.analysis.timing_metrics.overall_stop_time_utc": {
                                        "gte": timestartstring,
                                        "lt": timeendstring,
                                        "format": "yyy-MM-dd'T'HH:mm:ss"
                                    }
                                }
                            }
                        }
                    }
                ]
            }
        },
        "aggs": {
            "filtered_nested_timestamps": {
                "nested": {
                    "path": "specimen.samples.analysis"
                },
                "aggs": {
                    "filtered_range": {
                        "filter": {
                            "range": {
                                "specimen.samples.analysis.timing_metrics.overall_stop_time_utc": {
                                    "gte": timestartstring,
                                    "lt": timeendstring,
                                    "format": "yyy-MM-dd'T'HH:mm:ss"
                                }}
                        },
                        "aggs": {
                            "vmtype": {
                                "terms": {
                                    "field": "specimen.samples.analysis.host_metrics.vm_instance_type.raw",
                                    "size": 9999
                                },
                                "aggs": {
                                    "regions": {
                                        "terms": {
                                            "field": "specimen.samples.analysis.host_metrics.vm_region.raw",
                                            "size": 9999
                                        },
                                        "aggs": {
                                            "totaltime": {
                                                "sum": {
                                                    "field": "specimen.samples.analysis.timing_metrics.overall_walltime_seconds"
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }, size=9999)

    return es_resp


def get_previous_file_sizes(timeend, project):
    timeendstring = timeend.strftime('%Y-%m-%dT%H:%M:%S')
    es_resp = es.search(index='billing_idx', body={
        "query": {
            "bool": {
                "must": [
                    {
                        "term": {
                            "project.keyword": project
                        }
                    },
                    {
                        "range": {
                            "timestamp": {
                                "lt": timeendstring,
                            }
                        }
                    }
                ]

            }
        },
        "aggs": {
            "filtered_nested_timestamps": {
                "nested": {
                    "path": "specimen.samples.analysis"
                },
                "aggs": {
                    "sum_sizes": {
                        "sum": {
                            "field": "specimen.samples.analysis.workflow_outputs.file_size"
                        }
                    }
                }
            }
        }
    }, size=9999)
    return es_resp


def get_months_uploads(project, timefrom, timetil):

    timestartstring = timefrom.strftime('%Y-%m-%dT%H:%M:%S')
    timeendstring = timetil.strftime('%Y-%m-%dT%H:%M:%S')
    es_resp = es.search(index='billing_idx', body=
    {
        "query": {
            "bool": {
                "must": [
                    {
                        "range": {
                            "timestamp": {
                                "gte": timestartstring,
                                "lt": timeendstring
                            }
                        }
                    },
                    {
                        "term": {
                            "project.keyword": project
                        }
                    }
                ]
            }
        },
        "aggs": {
            "filtered_nested_timestamps": {
                "nested": {
                    "path": "specimen.samples.analysis"
                },
                "aggs": {
                    "times": {
                        "terms": {
                            "field": "specimen.samples.analysis.timestamp"
                        },
                        "aggs": {
                            "sum_sizes": {
                                "sum": {
                                    "field": "specimen.samples.analysis.workflow_outputs.file_size"
                                }
                            }
                        }
                    }
                }
            }
        }
    }, size=9999)
    return es_resp


@click.command()
@click.option("--date", default="", type=str)
def generate_daily_reports(date):
    # Need to pass app context around because of how flask works
    # can take a single argument date as follows
    # flask generate_daily_reports --date 2017/01/31 will compute the billings for jan 2017, up to the 31st day of
    # January

    try:
        timeend = datetime.datetime.strptime(date, '%Y/%m/%d')
    except:
        timeend = datetime.datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)

    # HANDLE CLOSING OUT BILLINGS at end of month
    if timeend.day == 1:
        projects = get_projects_list()
        for project in projects:
            bill = Billing.query.filter(Billing.end_data.month == (timeend.month - 1) % 12) \
                .filter(Billing.closed_out is False).filter(Billing.project == project).first()
            if bill:
                bill.update(end_date=timeend, closed_out=True)

    monthstart = timeend.replace(day=1)
    projects = get_projects_list()
    seconds_into_month = (timeend - monthstart).total_seconds()
    daysinmonth = calendar.monthrange(timeend.year, timeend.month)[1]
    portion_of_month = Decimal(seconds_into_month) / Decimal(daysinmonth * 3600 * 24)

    for project in projects:
        print(project)
        file_size = get_previous_file_sizes(monthstart, project=project)
        this_months_files = get_months_uploads(project, monthstart, timeend)
        compute_cost_search = make_search_filter_query(monthstart, timeend, project)
        compute_costs = get_compute_costs(compute_cost_search)
        analysis_compute_json = create_analysis_costs_json(compute_cost_search['hits']['hits'], monthstart, timeend)

        all_proj_files = get_previous_file_sizes(timeend, project)['hits']['hits']
        analysis_storage_json = create_storage_costs_json(all_proj_files, monthstart, timeend,
                                                          daysinmonth * 3600 * 24)
        storage_costs = get_storage_costs(file_size, portion_of_month,
                                          this_months_files, timeend, daysinmonth * 3600 * 24)

        bill = Billing.query.filter(Billing.project == project).filter(Billing.start_date == monthstart).first()
        itemized_costs = {
            "itemized_compute_costs": analysis_compute_json,
            "itemized_storage_costs": analysis_storage_json
        }
        if bill:
            bill.update(compute_cost=compute_costs, storage_cost=storage_costs, end_date=timeend,
                        cost_by_analysis=itemized_costs)
        else:
            Billing.create(compute_cost=compute_costs, storage_cost=storage_costs, start_date=monthstart,
                           end_date=timeend, project=project, closed_out=False,
                           cost_by_analysis=itemized_costs)

