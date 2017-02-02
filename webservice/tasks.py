from extensions import sqlalchemy, elasticsearch
import datetime
from datetime import timedelta
from utility import get_compute_costs, get_storage_costs
from models import Billing
import calendar
from decimal import Decimal
import logging
import click


def get_projects_list():
    from app import app
    with app.app_context():
        es_resp = elasticsearch.search(index='billing_idx', body={"query": {"match_all": {}}, "aggs": {
            "projects":{
                "terms":{
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
    from app import app
    with app.app_context():
        timestartstring = timefrom.strftime('%Y-%m-%dT%H:%M:%S')
        timeendstring = timetil.strftime('%Y-%m-%dT%H:%M:%S')
        es_resp = elasticsearch.search(index='billing_idx', body={
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
        }, size=0)

        return es_resp


def get_previous_file_sizes (timefrom, project):
    timestartstring = timefrom.strftime('%Y-%m-%dT%H:%M:%S')
    es_resp = elasticsearch.search(index='billing_idx', body={
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
                                "lt": timestartstring,
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
    }, size=0)
    return es_resp

def get_months_uploads(project, timefrom, timetil):
    from app import app
    with app.app_context():
        timestartstring = timefrom.strftime('%Y-%m-%dT%H:%M:%S')
        timeendstring = timetil.strftime('%Y-%m-%dT%H:%M:%S')
        es_resp = elasticsearch.search(index='billing_idx', body =
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
        }, size=0)
        return es_resp

@click.command()
def generate_daily_reports():
    #need to pass app context around because of how flask works

    from app import app
    with app.app_context():
        utcnow = datetime.datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
        monthstart = utcnow.replace(day=1)
        projects = get_projects_list()
        seconds_into_month = (utcnow-monthstart).total_seconds()
        daysinmonth = calendar.monthrange(utcnow.year, utcnow.month)[1]
        portion_of_month = Decimal(seconds_into_month)/Decimal(daysinmonth*3600*24)

        for project in projects:
            file_size = get_previous_file_sizes(monthstart, project=project)
            this_months_files = get_months_uploads(project, monthstart, utcnow)
            compute_costs = get_compute_costs(make_search_filter_query(monthstart,utcnow,project))
            storage_costs = get_storage_costs( file_size, portion_of_month,
                              this_months_files, utcnow, daysinmonth*3600*24)
            bill = Billing.query.filter(Billing.project == project).filter(Billing.start_date == monthstart).first()
            if bill:
                bill.update(compute_cost=compute_costs, storage_cost=storage_costs, end_date=utcnow)
            else:
                Billing.create(compute_cost=compute_costs, storage_cost=storage_costs, start_date=monthstart,\
                               end_date=utcnow, project=project, closed_out=False)


@click.command()
def close_out_billings():
    #need to pass app context around because of how flask works
    from app import app
    with app.app_context():
        projects = get_projects_list()
        utcnow = datetime.datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
        for project in projects:
            bill = Billing.query.filter(Billing.project == project).filter(Billing.closed_out is False).first()
            if bill:
                bill.update(end_date=utcnow, closed_out=True)

            Billing.create(project=project, cost=0, start_date=utcnow, end_date=utcnow, closed_out=False)
