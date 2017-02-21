# Alex Hancock, UCSC CGL
# 
# Luigi Monitor

import json
import urllib2
import boto

from boto.s3.key import Key
from datetime 	 import datetime, timedelta
from sqlalchemy  import *

def getTouchfile(touchfile_name):
	s3 = boto.connect_s3()
	bucket_name = 'cgl-core-analysis-run-touch-files'

	bucket = s3.get_bucket(bucket_name)

	k = Key(bucket)
	k.key = touchfile_name
	contents = k.get_contents_as_string()
	return contents

# 
# Luigi Scraping below
# 
def getJobList():
	server = "http://localhost:8082/api/"

	running_url   = server + "task_list?data=%7B%22status%22%3A%22RUNNING%22%2C%22upstream_status%22%3A%22%22%2C%22search%22%3A%22%22%7D"
	batch_url     = server + "task_list?data=%7B%22status%22%3A%22BATCH_RUNNING%22%2C%22upstream_status%22%3A%22%22%2C%22search%22%3A%22%22%7D"
	failed_url    = server + "task_list?data=%7B%22status%22%3A%22FAILED%22%2C%22upstream_status%22%3A%22%22%2C%22search%22%3A%22%22%7D"
	upfail_url    = server + "task_list?data=%7B%22status%22%3A%22PENDING%22%2C%22upstream_status%22%3A%22UPSTREAM_FAILED%22%2C%22search%22%3A%22%22%7D"
	disable_url   = server + "task_list?data=%7B%22status%22%3A%22DISABLED%22%2C%22upstream_status%22%3A%22%22%2C%22search%22%3A%22%22%7D"
	updisable_url = server + "task_list?data=%7B%22status%22%3A%22PENDING%22%2C%22upstream_status%22%3A%22UPSTREAM_DISABLED%22%2C%22search%22%3A%22%22%7D"
	pending_url   = server + "task_list?data=%7B%22status%22%3A%22PENDING%22%2C%22upstream_status%22%3A%22%22%2C%22search%22%3A%22%22%7D"
	done_url      = server + "task_list?data=%7B%22status%22%3A%22DONE%22%2C%22upstream_status%22%3A%22%22%2C%22search%22%3A%22%22%7D"

	list_of_URLs = [running_url, batch_url, failed_url, upfail_url, 
	                disable_url, updisable_url, pending_url, done_url]

	relevant_attributes = ["status", "name", "start_time", "params"]
	required_parameters = ["project", "donor_id", "sample_id", "pipeline_name"]

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
	    req = urllib2.Request(URL)
	    response = urllib2.urlopen(req)
	    text_tools = response.read()
	    json_tools = json.loads(text_tools)

	    luigi_job_list = json_tools["response"]

	    if not luigi_job_list:
	        # Just skip an empty response
	        continue

	    for job in luigi_job_list:
	    	local_job_list[job] = luigi_job_list[job]

	return local_job_list

def proxyConversion(resultProxy):
	return [row for row in resultProxy]

#
# Database initialization, creation if table doesn't exist
#
# Change echo to True to show SQL code... unnecessary
# Any time facets?
# When started:
# When ended:
# Last touched:
# Time elapsed:
db = create_engine('postgresql:///monitor', echo=True)
conn = db.connect()
metadata = MetaData(db)
luigi = Table('luigi', metadata,
	Column("luigi_job", String(100), primary_key=True),
	Column("status", String(20)),

	Column("submitter_specimen_id", String(40)),
	Column("specimen_uuid", String(60)),
	Column("workflow_name", String(40)),
	Column("center_name", String(40)),
	Column("submitter_donor_id", String(40)),
	Column("consonance_job_uuid", String(40)),
	Column("submitter_donor_primary_site", String(40)),
	Column("project", String(40)),
	Column("analysis_type", String(40)),
	Column("program", String(40)),
	Column("donor_uuid", String(60)),
	Column("submitter_sample_id", String(40)),
	Column("submitter_experimental_design", String(40)),
	Column("submitter_specimen_type", String(40)),
	Column("workflow_version", String(40)),
	Column("sample_uuid", String(60)),

	Column("start_time", Float),
	Column("last_updated", Float)
)
if not db.dialect.has_table(db, luigi):
	luigi.create()

jobList = getJobList()
for job in jobList:
	job_dict = jobList[job]	
	#print job
	#print job_dict['status']
	#
	# S3 Scraping below
	#
	touchfile_name = job_dict['params']['touch_file_path'] + '/' + \
					 job_dict['params']['submitter_sample_id'] + \
					 '_meta_data.json'
	#print touchfile_name
	stringContents = getTouchfile(touchfile_name)
	jsonMetadata = json.loads(stringContents)

	select_query = select([luigi]).where(luigi.c.luigi_job == job)
	select_exist_result = proxyConversion(conn.execute(select_query))
	#print type(select_exist_result)
	#print "RESULT:", select_exist_result
	if len(select_exist_result) == 0:
		#	insert into db	
		ins_query = luigi.insert().values(luigi_job=job,
						status=job_dict['status'],
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
						sample_uuid=jsonMetadata['sample_uuid'],
						start_time=job_dict['start_time'],
						last_updated=job_dict['last_updated'])
		exec_result = conn.execute(ins_query)	
		# Uhhh... some error throwing on exec_result? 
	else:
		row = select_exist_result[0]
		# row[1] is the status of the job
		if (row[1] == job_dict['status']):
			if row[1] == "RUNNING":
				stmt = luigi.update().\
					   where(luigi.c.luigi_job == job).\
					   values(last_updated=job_dict['last_updated'])
				exec_result = conn.execute(stmt)
			else:
				# STILL DONE OR FAILED
				continue
		else: 
			# Status has changed
			stmt = luigi.update().\
				   where(luigi.c.luigi_job == job).\
				   values(status=job_dict['status'], last_updated=job_dict['last_updated'])
			exec_result = conn.execute(stmt)
			# Update status change, time finished, time elapsed
			# Update with logs and auth later
