# Alex Hancock, UCSC CGL
# 
# Luigi Monitor

import json
import urllib2
import boto

from boto.s3.key import Key
from datetime 	 import datetime, timedelta
from sqlalchemy  import *

def getTouchfile(bucket_name, touchfile_name):
	s3 = boto.connect_s3()

	bucket = s3.get_bucket(bucket_name)

	k = Key(bucket)
	k.key = touchfile_name
	contents = k.get_contents_as_string()
	return contents

# 
# Luigi Scraping below
# 

bucket_name = 'abhancoc-luigi-monitor-touch-files'
touchfile_name = 'metadata.json'

#
# S3 Scraping below
#

stringContents = getTouchfile(bucket_name, touchfile_name)
jsonMetadata = json.loads(stringContents)

print jsonMetadata['specimen'][0]['submitter_experimental_design']
#
# Database Segment below
#

db = create_engine('postgresql:///hancock')
db.echo = True  # Try changing this to True and see what happens
metadata = MetaData(db)

# Other attributes:
# 	Instance
# 	How long?
# 	Start/end

luigi = Table('luigi', metadata,
    #Column('consonance_uuid', String(40), primary_key=True),
    Column('project', String(40)),
    Column('program', String(40)),
    Column('donor', String(40)),
    Column('specimen', String(40)),
    Column('sample', String(40)),
    #Column('status', String(40)),
    #Column('analysis_type', String(40)),
    Column('workflow_name', String(40)),
    #Column('luigi_link', String),
)
#luigi.create()

i = luigi.insert()
#i.execute(name='Mary', age=30, password='secret')
i.execute({'project': 		jsonMetadata['project'],
		   'program': 		jsonMetadata['program'],
		   'donor': 		jsonMetadata['submitter_donor_id'],
		   'specimen': 		jsonMetadata['specimen'][0]['specimen_uuid'],
		   'sample':   		jsonMetadata['specimen'][0]['samples'][0]['sample_uuid'],
		   'workflow_name': jsonMetadata['specimen'][0]['submitter_experimental_design']
		   })
          

#s = luigi.select()
#rs = s.execute()

#row = rs.fetchone()
#print 'Id:', row[0]
#print 'Name:', row['name']
#print 'Age:', row.age
#print 'Password:', row[luigi.c.password]

#for row in rs:
#    print row.name, 'is', row.age, 'years old'
