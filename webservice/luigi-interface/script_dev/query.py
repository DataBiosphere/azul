# Alex Hancock, UCSC CGL

import json
from sqlalchemy  import *

def proxyConversion(resultProxy):
	save = []
	for row in resultProxy:
		save.append(row)
	return save

#
# Database initialization, creation if table doesn't exist
#
# Change echo to True to show SQL code... unnecessary
# Any time facets?
# When started:
# When ended:
# Last touched:
# Time elapsed:
db = create_engine('postgresql:///monitor', echo=False)
db = create_engine('postgresql:///hancock', echo=False)
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
	Column("consonance_id", String(40)),
	#Column("consonance_job_uuid", String(40)),
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

select_query = select([luigi])
select_exist_result = conn.execute(select_query)
for row in select_exist_result:
	print row
