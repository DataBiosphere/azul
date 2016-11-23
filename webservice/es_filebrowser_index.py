#indexer_es_fb.py

#start virtual environment
#>>> pip install jsonlines, ast, json
#need to be in same folder as validated.jsonl
#>>> python2.7 indexer_es_fb.py
#produces a fb_index.jsonl file to be added to elasticsearch

#This takes the "validated.jsonl" file produced by the many scripts in the Fall Demo Script
import jsonlines, ast, json, luigi
from elasticsearch import Elasticsearch

counter = 0;
es = Elasticsearch()


redwood_host = luigi.Parameter(default='storage.ucsc-cgl.org') # Put storage instead of storage2
bundle_uuid_filename_to_file_uuid = {}

def requires():
        print "** COORDINATOR **"
        # now query the metadata service so I have the mapping of bundle_uuid & file names -> file_uuid
        print str("https://"+redwood_host+":8444/entities?page=0")
        json_str = urlopen(str("https://"+redwood_host+":8444/entities?page=0")).read()
        metadata_struct = json.loads(json_str)
        print "** METADATA TOTAL PAGES: "+str(metadata_struct["totalPages"])
        for i in range(0, metadata_struct["totalPages"]):
            print "** CURRENT METADATA TOTAL PAGES: "+str(i)
            json_str = urlopen(str("https://"+redwood_host+":8444/entities?page="+str(i))).read()
            metadata_struct = json.loads(json_str)
            for file_hash in metadata_struct["content"]:
                bundle_uuid_filename_to_file_uuid[file_hash["gnosId"]+"_"+file_hash["fileName"]] = file_hash["id"]
        print bundle_uuid_filename_to_file_uuid        

print "Entering the method"
requires()

with open("fb_index.jsonl", "w") as fb_index: 
   #metadata = open("validated.jsonl", "r")
   #with jsonlines.open("validated.jsonl") as reader:
   #Call ES instead of having the hardcoded file.
      m_text = es.search(index='analysis_index', body={"query":{"match_all":{}}}, scroll="1m")
      reader = [x['_source'] for x in m_text['hits']['hits']]
      #print reader2   
      for obj in reader:
         #pull out center name, project, program, donor(submitter_donor_id)
         center_name = obj['center_name']
         project = obj['project']
         program = obj['program']
         donor = obj['submitter_donor_id']
         #go to specimen
         for speci in obj['specimen']:
            #pull out specimen_type(submitter_specimen_type)
            specimen_type = speci['submitter_specimen_type']
            for sample in speci['samples']:
               for analys in sample['analysis']:
                  # pull out analysis_type, workflow(workflow_name), download_id(bundle_uuid)
                  analysis_type = analys['analysis_type']
                  workflow = analys['workflow_name']
                  download_id = analys['bundle_uuid']
                  for file in analys['workflow_outputs']:
                     #pull out file_type, title(file_path)
                     file_type = file['file_type']
                     title = file['file_path']
                     #creating the header
                     indexing = {"index":{"_id": counter, "_type":"meta"}}
                     indexing = str(indexing).replace("'",'"')
                     counter += 1
                     #add all stuff to dictionary
                     udict = {'center_name': center_name, 'project': project, 'program': program, 'donor': donor, 'specimen_type': specimen_type, 'analysis_type': analysis_type, 'workflow': workflow, 'download_id': download_id, 'file_type': file_type, 'title': title}
                     adict = ast.literal_eval(json.dumps(udict))
                     adict = str(adict).replace("'",'"')
                     #push header and dictionary to .jsonl
                     fb_index.write(indexing+"\n"+adict+"\n")