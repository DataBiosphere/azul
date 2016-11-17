#indexer_es_fb.py

#start virtual environment
#>>> pip install jsonlines, ast, json
#need to be in same folder as validated.jsonl
#>>> python2.7 indexer_es_fb.py
#produces a fb_index.jsonl file to be added to elasticsearch

#This takes the "validated.jsonl" file produced by the many scripts in the Fall Demo Script
import jsonlines, ast, json

counter = 0;
with open("fb_index.jsonl", "w") as fb_index: 
   #metadata = open("validated.jsonl", "r")
   with jsonlines.open("validated.jsonl") as reader:
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