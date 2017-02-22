# Alex Hancock, UCSC CGL
# 
# Luigi Monitor

import json
import boto

from boto.s3.key import Key
from sqlalchemy  import *

unique_job = 'spawnFlop_SRR1988343_demo__consonance_jobs__0992701f6f'
sample_id = 'DTB-116_Baseline_1' 
topfolder = 'UCSF_SU2C_WCDT_DTB-116_DTB-116_Baseline'

def getTouchfile(touchfile_name):
	s3 = boto.connect_s3()
	bucket_name = 'cgl-core-analysis-run-touch-files'

	bucket = s3.get_bucket(bucket_name)

	k = Key(bucket)
	k.key = touchfile_name
	contents = k.get_contents_as_string()
	return contents

#
# Database initialization, creation if table doesn't exist
#
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

#
# S3 Scraping below
#
filepath = 'consonance-jobs/RNASeq_3_1_x_Coordinator/3_1_3/' + topfolder
touchfile_name = filepath + '/' + \
				 sample_id + \
				 '_meta_data.json'

stringContents = getTouschfile(touchfile_name)
jsonMetadata = json.loads(stringContents)

ins_query = luigi.insert().values(luigi_job=unique_job,
				status="DONE",
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
				start_time=1487716334.29525,
				last_updated=1487716634.38815)
exec_result = conn.execute(ins_query)
print exec_result