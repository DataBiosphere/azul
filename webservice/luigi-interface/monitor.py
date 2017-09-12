# Alex Hancock, UCSC CGL
#
# Luigi Monitor

import boto
import json
import os
import subprocess
import sys
import urllib2

from sqlalchemy import create_engine, MetaData, Table, Column, String, select, and_
from datetime import datetime


def get_touchfile(bucket_name, touchfile_name):
    s3 = boto.connect_s3()
    bucket = s3.get_bucket(bucket_name, validate=False)
    print "GOT S3 BUCKET" 

    key = bucket.new_key(touchfile_name)
    print "CREATED NEW S3 KEY"

    contents = key.get_contents_as_string()
    print "GOT S3 FILE CONTENTS"

    return contents


def get_job_list():
    # Scrape Luigi
    server = os.getenv("LUIGI_SERVER") + ":" + os.getenv("LUIGI_PORT", "8082") + "/api/"
    running_url = server + "task_list?data=%7B%22status%22%3A%22RUNNING%22%2C%22upstream_status%22%3A%22%22%2C%22search%22%3A%22%22%7D"
    batch_url = server + "task_list?data=%7B%22status%22%3A%22BATCH_RUNNING%22%2C%22upstream_status%22%3A%22%22%2C%22search%22%3A%22%22%7D"
    failed_url = server + "task_list?data=%7B%22status%22%3A%22FAILED%22%2C%22upstream_status%22%3A%22%22%2C%22search%22%3A%22%22%7D"
    upfail_url = server + "task_list?data=%7B%22status%22%3A%22PENDING%22%2C%22upstream_status%22%3A%22UPSTREAM_FAILED%22%2C%22search%22%3A%22%22%7D"
    disable_url = server + "task_list?data=%7B%22status%22%3A%22DISABLED%22%2C%22upstream_status%22%3A%22%22%2C%22search%22%3A%22%22%7D"
    updisable_url = server + "task_list?data=%7B%22status%22%3A%22PENDING%22%2C%22upstream_status%22%3A%22UPSTREAM_DISABLED%22%2C%22search%22%3A%22%22%7D"
    pending_url = server + "task_list?data=%7B%22status%22%3A%22PENDING%22%2C%22upstream_status%22%3A%22%22%2C%22search%22%3A%22%22%7D"
    done_url = server + "task_list?data=%7B%22status%22%3A%22DONE%22%2C%22upstream_status%22%3A%22%22%2C%22search%22%3A%22%22%7D"

    list_of_URLs = [running_url, batch_url, failed_url, upfail_url,
                    disable_url, updisable_url, pending_url, done_url]

    local_job_list = {}
    for URL in list_of_URLs:
        name = URL[62:]
        suffix = ""
        if "UPSTREAM" in name:
            if "FAILED" in name:
                name = "UPSTREAM_FAILED"
            else:
                name = "UPSTREAM_DISABLED"
        else:
            name = name.split("%")[0] + suffix

        # Retrieve api tool dump from URL and read it into json_tools
        print "URL: ", URL
        req = urllib2.Request(URL)
        response = urllib2.urlopen(req)
        text_tools = response.read()
        print "TEXT TOOLS:", text_tools
        json_tools = json.loads(text_tools)

        luigi_job_list = json_tools["response"]

        if not luigi_job_list:
            # Just skip an empty response
            continue

        for job in luigi_job_list:
            local_job_list[job] = luigi_job_list[job]

    return local_job_list


def query_to_list(result_proxy):
    # Can use iterator to create list,
    # but result from SQLAlchemy isn't
    # great for other techniques used here
    return [row for row in result_proxy]


def get_consonance_status(consonance_uuid):
    cmd = ['consonance', 'status', '--job_uuid', str(consonance_uuid)]
    status_text = subprocess.check_output(cmd)
    return json.loads(status_text)


# This was exported to a method to avoid duplication
# for both the creation and update timestamps
def format_consonance_timestamp(consonance_timestamp):
    datetime_obj = datetime.strptime(consonance_timestamp, '%Y-%m-%dT%H:%M:%S.%f+0000')
    return datetime.strftime(datetime_obj, '%Y-%m-%d %H:%M')


#
# Database initialization, creation if table doesn't exist
#
# Change echo to True to show SQL code... unnecessary for production
#
db = create_engine('postgresql://{}:{}@db/{}'.format(os.getenv("POSTGRES_USER"), os.getenv("POSTGRES_PASSWORD"), os.getenv("POSTGRES_DB")), echo=False)
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
              Column("last_updated", String(100)))

if not db.dialect.has_table(db, luigi):
    luigi.create()

jobList = get_job_list()

for job in jobList:
    job_dict = jobList[job]

    #
    # S3 scraping below
    #
    try:
        s3string = job_dict['params']['touch_file_path']
        bucket_name, filepath = s3string.split('/', 1)
        touchfile_name = filepath + '/' + job_dict['params']['metadata_json_file_name']
        print "BUCKET_NAME:", bucket_name
        print "TOUCH FILE NAME:", touchfile_name
        stringContents = get_touchfile(bucket_name, touchfile_name)
        print "GOT STRING CONTENTS", stringContents
        jsonMetadata = json.loads(stringContents)
        print "LOADED JSON DATA", jsonMetadata
    except Exception as e:
        # Hardcoded jsonMetadata
        print >>sys.stderr, e.message, e.args
        print >>sys.stderr, "Failure when connecting to S3, dumping job dictionary:", job_dict
        continue

    #
    # Consonance scraping below
    #
    print "GETTING CONSONANCE STATUS FOR METADATA:", jsonMetadata
    print "and job:", job
    try:
        # Use uuid from S3
        status_json = get_consonance_status(jsonMetadata['consonance_job_uuid'])
    except:
        # Default to Luigi status and timestamps
        print "EXCEPT STATEMENT IN GETTING CONSONANCE STATUS FOR JOB WITH METADATA", job
        status_json = {
            'create_timestamp': job_dict['start_time'],
            'update_timestamp': job_dict['last_updated'],
            'state': 'LUIGI:' + job_dict['status']
        }

    #
    # Find if current job is already listed in
    # job database, insert if absent
    #

    print "SELECTING JOB:", job
    # use the Consonance job uuid instead of the Luigi job id because
    # Luigi sometimes reuses job ids for different runs
    # The Consonance job uuid is always unique
#    select_query = select([luigi]).where(luigi.c.luigi_job == job)
    job_uuid = jsonMetadata['consonance_job_uuid']
    print "QUERYING DB FOR JOB UUID:", job_uuid
    select_query = select([luigi]).where(luigi.c.consonance_job_uuid == job_uuid)
    select_result = query_to_list(conn.execute(select_query))
    print "JOB RESULT:", select_result
    if len(select_result) == 0:
        try:
            #ins_query = luigi.insert().values(luigi_job=job,
            ins_query = luigi.insert().values(luigi_job=job_uuid,
                                              submitter_specimen_id=jsonMetadata['submitter_specimen_id'],
                                              specimen_uuid=jsonMetadata['specimen_uuid'],
                                              workflow_name=jsonMetadata['workflow_name'],
                                              center_name=jsonMetadata['center_name'],
                                              submitter_donor_id=jsonMetadata['submitter_donor_id'],
                                              consonance_job_uuid=jsonMetadata['consonance_job_uuid'],
                                              submitter_donor_primary_site=jsonMetadata['submitter_donor_primary_site'],
                                              project=jsonMetadata['project'],
                                              analysis_type=jsonMetadata['analysis_type'],
                                              program=jsonMetadata['program'],
                                              donor_uuid=jsonMetadata['donor_uuid'],
                                              submitter_sample_id=jsonMetadata['submitter_sample_id'],
                                              submitter_experimental_design=jsonMetadata['submitter_experimental_design'],
                                              submitter_specimen_type=jsonMetadata['submitter_specimen_type'],
                                              workflow_version=jsonMetadata['workflow_version'],
                                              sample_uuid=jsonMetadata['sample_uuid'])
            exec_result = conn.execute(ins_query)
        except Exception as e:
            print >>sys.stderr, e.message, e.args
            print "Dumping jsonMetadata to aid debug:\n", jsonMetadata
            continue

#
# Get Consonance status for each entry in our db
#
# Select all from the table, pipe results into a list
#
# for job in list
#     consonance status using job.consonance_uuid
#     update that job using the information from status return
#
select_query = select([luigi])
select_result = conn.execute(select_query)
result_list = [dict(row) for row in select_result]
for job in result_list:
    try:
        job_name = job['luigi_job']
        job_uuid = job['consonance_job_uuid']

        if job_uuid == "no consonance id in test mode":
            # Skip test mode Consonance ID's
            # and force next job
            print "\nTest ID, skipping"

            stmt = luigi.delete().where(luigi.c.luigi_job == job_name)
            exec_result = conn.execute(stmt)
        else:
            # Consonace job id is real
            print "\nJOB NAME:", job_uuid

            status_json = get_consonance_status(job_uuid)
            state = status_json['state']
            created = format_consonance_timestamp(status_json['create_timestamp'])
            updated = format_consonance_timestamp(status_json['update_timestamp'])

            # DEBUG to check if state, created, and updated are collected
            print "STATE:", state
            print "CREATED:", created
            print "UPDATED:", updated

            stmt = luigi.update().\
                where(luigi.c.luigi_job == job_name).\
                values(status=status_json['state'],
                       start_time=created,
                       last_updated=updated)
            exec_result = conn.execute(stmt)

    except Exception as e:
        print >>sys.stderr, e.message, e.args
        print >>sys.stderr, "Dumping job entry:", job

        stmt = luigi.update().\
            where((and_(luigi.c.luigi_job == job_name,
                        luigi.c.status != 'SUCCESS',
                        luigi.c.status != 'FAILED'))).\
            values(status='JOB NOT FOUND')
        exec_result = conn.execute(stmt)
