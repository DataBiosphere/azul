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
@click.option("--date", default="", type=str)
def generate_daily_reports(date):
    # Need to pass app context around because of how flask works
    # can take a single argument date as follows
    # flask generate_daily_reports --date 2017/01/31 will compute the billings for jan 2017, up to the 31st day of
    # January

    from app import app
    with app.app_context():
        try:
            timeend = datetime.datetime.strptime(date, '%Y/%m/%d')
        except:
            timeend = datetime.datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)



        # HANDLE CLOSING OUT BILLINGS at end of month
        if timeend.day == 1:
            projects = get_projects_list()
            for project in projects:
                bill = Billing.query.filter(Billing.end_data.month == (timeend.month-1) % 12)\
                    .filter(Billing.closed_out is False).filter(Billing.project == project).first()
                if bill:
                    bill.update(end_date=timeend, closed_out=True)


        monthstart = timeend.replace(day=1)
        projects = get_projects_list()
        seconds_into_month = (timeend-monthstart).total_seconds()
        daysinmonth = calendar.monthrange(timeend.year, timeend.month)[1]
        portion_of_month = Decimal(seconds_into_month)/Decimal(daysinmonth*3600*24)

        for project in projects:
            print(project)
            file_size = get_previous_file_sizes(monthstart, project=project)
            this_months_files = get_months_uploads(project, monthstart, timeend)
            compute_costs = get_compute_costs(make_search_filter_query(monthstart,timeend,project))
            storage_costs = get_storage_costs( file_size, portion_of_month,
                              this_months_files, timeend, daysinmonth*3600*24)
            bill = Billing.query.filter(Billing.project == project).filter(Billing.start_date == monthstart).first()
            if bill:
                bill.update(compute_cost=compute_costs, storage_cost=storage_costs, end_date=timeend)
            else:
                Billing.create(compute_cost=compute_costs, storage_cost=storage_costs, start_date=monthstart,\
                               end_date=timeend, project=project, closed_out=False)
