# Alex Hancock, UCSC CGL
# 
# Luigi Monitor

import json
import os
import sys

from datetime 	 import datetime
from sqlalchemy  import *

#
# Database initialization, creation if table doesn't exist
#
# Change echo to True to show SQL code... unnecessary
db = create_engine('postgresql://{}:{}@db/monitor'.format(os.getenv("POSTGRES_USER"), os.getenv("POSTGRES_PASSWORD")), echo=False)
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
if not db.dialect.has_table(db, luigi):
	luigi.create()

# 
# for job in list
#     consonance status using job.consonance_uuid
#     update that job using the information from status return
select_query = select([luigi])
select_result = conn.execute(select_query)
result_list = [dict(row) for row in select_result]
for job in result_list:
	try:
		job_name = job['luigi_job']
		job_uuid = job['consonance_job_uuid']

		if job_uuid == "no consonance id in test mode":
			continue
		else:
			# Delete old jobs
			current_month = datetime.now().month
			job_month = int(updated[5:7])

			# Delete jobs older than one month old, ignore if 
			# jobs were updated in December and it's January
			if ((abs(job_month - current_month) > 1)
				and (job_month + current_month != 13)):
				stmt = luigi.delete().\
					   where(luigi.c.luigi_job == job_name)
				exec_result = conn.execute(stmt)

	except Exception as e:
		print >>sys.stderr, "ERROR:", str(e)
