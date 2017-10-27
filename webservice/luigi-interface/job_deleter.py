# Alex Hancock, UCSC CGL
# 
# Luigi Monitor

import json
import os
import sys

from datetime import datetime
from sqlalchemy import select

# Add parent directory to get luigidb init
sys.path.append( os.path.dirname( os.path.dirname( os.path.abspath(__file__) ) ) )
from monitordb_lib import luigiDBInit


monitordb_connection, monitordb_table, db_engine = luigiDBInit()

# 
# for job in list
#     consonance status using job.consonance_uuid
#     update that job using the information from status return
select_query = select([monitordb_table])
select_result = monitordb_connection.execute(select_query)
result_list = [dict(row) for row in select_result]
for job in result_list:
	try:
		job_uuid = job['consonance_job_uuid']
		updated  = job['last_updated']

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
				stmt = monitordb_table.delete().\
					where(monitordb_table.c.consonance_job_uuid == job_uuid)
				exec_result = monitordb_connection.execute(stmt)
	except Exception as e:
		print >>sys.stderr, "ERROR:", str(e)

monitordb_connection.close()
db_engine.dispose()
