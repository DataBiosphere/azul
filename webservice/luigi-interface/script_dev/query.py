# Alex Hancock, UCSC CGL

import json
from sqlalchemy import create_engine, MetaData, String, Table, Float, Column, select

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
print result_list