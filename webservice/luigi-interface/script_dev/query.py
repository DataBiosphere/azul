# Alex Hancock, UCSC CGL

import json
import subprocess
import sys
from sqlalchemy import create_engine, MetaData, String, Table, Float, Column, select

def get_consonance_status(consonance_uuid):
	cmd = ['consonance', 'status', '--job_uuid', consonance_uuid]
	status_text = subprocess.check_output(cmd)
	return json.loads(status_text)

db = create_engine('postgresql:///monitor', echo=False)
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

	Column("start_time", Float),
	Column("last_updated", Float)
)

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
			print "Test ID, skipping"
			continue
		else:
			# Consonace job id is real
			print "\nJOB NAME:", job_uuid

			status_json = get_consonance_status(job_uuid)

			state = status_json['state']
			created = status_json['create_timestamp']
			updated = status_json['update_timestamp']

			print "STATE:", state
			print "CREATED:", created
			print "UPDATED:", updated
			# DEBUG, comment when testing
			continue

			stmt = luigi.update().\
				   where(luigi.c.luigi_job == job_name).\
				   values(status=state, 
				   		  last_updated=updated,
				   		  start_time=created)
			exec_result = conn.execute(stmt)
	except Exception as e:
		print >>sys.stderr, "ERROR:", str(e)