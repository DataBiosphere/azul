#indexer_es_fb.py

#start virtual environment
#>>> pip install jsonlines, ast, json
#need to be in same folder as validated.jsonl
#>>> python2.7 indexer_es_fb.py
#produces a fb_index.jsonl file to be added to elasticsearch

#This takes the "validated.jsonl" file produced by the many scripts in the Fall Demo Script
import jsonlines, ast, json, luigi, ssl, argparse
from elasticsearch import Elasticsearch
from urllib import urlopen

counter = 0;
es = Elasticsearch()


redwood_host = 'storage.ucsc-cgl.org'#redwood_host = luigi.Parameter(default='storage.ucsc-cgl.org') # Put storage instead of storage2
bundle_uuid_filename_to_file_uuid = {}
#index_size = 0


parser = argparse.ArgumentParser(description='Process options for the remaining index fields.')
parser.add_argument('--access', dest='access', action='store',
                    default='public', help='The access type for the files; <controlled | public>. Defaults to public.')
parser.add_argument('--repoBaseUrl', dest='repoBaseUrl', action='store',
                    default='storage.ucsc-cgl.org', help='The url for the storage system. Defaults to storage.ucsc-cgl.org')
parser.add_argument('--repoCode', dest='repoCode', action='store',
                    default='Redwood-AWS-Oregon', help='The code for the repo. Defaults to Redwood-AWS-Oregon')
parser.add_argument('--repoCountry', dest='repoCountry', action='store',
                    default='US', help='The country for the repo. Defaults to US')
parser.add_argument('--repoName', dest='repoName', action='store',
                    default='Redwood-AWS-Oregon', help='The name for the repo. Defaults to Redwood-AWS-Oregon')
parser.add_argument('--repoOrg', dest='repoOrg', action='store',
                    default='UCSC', help='The organization for the repo. Defaults to UCSC')
parser.add_argument('--repoType', dest='repoType', action='store',
                    default='Redwood', help='The type for the repo. Defaults to Redwood')
#Get the arguments into args
args = parser.parse_args()



def requires():
        print "** COORDINATOR **"
       # print redwood_host
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        # now query the metadata service so I have the mapping of bundle_uuid & file names -> file_uuid
        print str("https://"+redwood_host+":8444/entities?page=0")
        json_str = urlopen(str("https://"+redwood_host+":8444/entities?page=0"), context=ctx).read()
        metadata_struct = json.loads(json_str)
        print "** METADATA TOTAL PAGES: "+str(metadata_struct["totalPages"])
        for i in range(0, metadata_struct["totalPages"]):
            print "** CURRENT METADATA TOTAL PAGES: "+str(i)
            json_str = urlopen(str("https://"+redwood_host+":8444/entities?page="+str(i)), context=ctx).read()
            metadata_struct = json.loads(json_str)
            for file_hash in metadata_struct["content"]:
                bundle_uuid_filename_to_file_uuid[file_hash["gnosId"]+"_"+file_hash["fileName"]] = file_hash["id"]

        #print bundle_uuid_filename_to_file_uuid
                # HACK!!!  Please remove once the behavior has been fixed in the workflow!!
                if file_hash["fileName"].endswith(".sortedByCoord.md.bam"):
                    bundle_uuid_filename_to_file_uuid[file_hash["gnosId"] + "_sortedByCoord.md.bam"] = file_hash[
                        "id"]
                if file_hash["fileName"].endswith(".tar.gz"):
                    bundle_uuid_filename_to_file_uuid[file_hash["gnosId"] + "_tar.gz"] = file_hash[
                        "id"]
                if file_hash["fileName"].endswith(".wiggle.bg"):
                    bundle_uuid_filename_to_file_uuid[file_hash["gnosId"] + "_wiggle.bg"] = file_hash[
                        "id"]
        # print bundle_uuid_filename_to_file_uuid
        # index_size = len(bundle_uuid_filename_to_file_uuid)
        # print index_size #TEST        

print "Entering the method"
requires()

with open("fb_index.jsonl", "w") as fb_index: 
   #metadata = open("validated.jsonl", "r")
   #with jsonlines.open("validated.jsonl") as reader:
   #Call ES instead of having the hardcoded file.
      # print "The index size is: ", len(bundle_uuid_filename_to_file_uuid)
      m_text = es.search(index='analysis_index', body={"query":{"match_all":{}}}, size=9999, scroll='2m')
      #Get the scroll id and total scroll size
      sid = m_text['_scroll_id']
      scroll_size = m_text['hits']['total']
      reader = [x['_source'] for x in m_text['hits']['hits']]

      while(scroll_size > 0):
        print "Scrolling..."
        page = es.scroll(scroll_id = sid, scroll = '2m')
        #Update the Scroll ID
        sid = page['_scroll_id']
        #Get the number of results that we returned in the last scroll
        scroll_size = len(page['hits']['hits'])
        #Extend the reader list
        reader.extend([x['_source'] for x in page['hits']['hits']])
        print len(reader)
        print "Scroll Size: " + str(scroll_size)


      #reader = [x['_source'] for x in m_text['hits']['hits']]
      # print reader #TEST
      # print len(reader)   
      for obj in reader:
         #pull out center name, project, program, donor(submitter_donor_id)
         center_name = obj['center_name']
         project = obj['project']
         study = obj['project']
         program = obj['program']
         donor = obj['donor_uuid']#obj['submitter_donor_id']
         redwoodDonorUUID = obj['donor_uuid']
         submittedDonorId = obj['submitter_donor_id']
         #Use lambda function to get the value or return empty if not present
         getValue = lambda x,y: x[y] if y in x else ''
         submitterDonorPrimarySite = getValue(obj, "submitter_donor_primary_site")
         #go to specimen
         for speci in obj['specimen']:
            #pull out specimen_type(submitter_specimen_type)
            specimen_type = speci['submitter_specimen_type']
            submitter_experimental_design = speci['submitter_experimental_design'] #Get the 'experimentalStrategy'
            submittedSpecimenId = speci['submitter_specimen_id']
            specimenUUID = speci['specimen_uuid']
            for sample in speci['samples']:
              sampleId = sample['sample_uuid']
              submittedSampleId = sample['submitter_sample_id']
              for analys in sample['analysis']:
                  # pull out analysis_type, workflow(workflow_name), download_id(bundle_uuid)
                  analysis_type = analys['analysis_type']
                  workflow = analys['workflow_name']
                  workflow_version = analys['workflow_version']
                  software = analys['workflow_name']
                  #Bundle UUID
                  repoDataBundleId = ''
                  if 'bundle_uuid' in analys:
                    repoDataBundleId = analys['bundle_uuid']
                  #Timestamp / lastModified; Empty if not present
                  lastModified = None
                  if 'timestamp' in analys:
                    lastModified = analys['timestamp']

                 #TEST WORKFLOW CONCATENATION
                  workflow = workflow+':'+workflow_version #DELETE IF IT CRASHES
                  download_id = analys['bundle_uuid']
                  for file in analys['workflow_outputs']:
                     #pull out file_type, title(file_path)
                     file_type = file['file_type']
                     title = file['file_path']
                     #Doing ifs because I don't know if it is in all the workflow outputs
                     fileSize = 0
                     fileMd5sum = ''
                     if 'file_size' in file:
                      fileSize = file['file_size']
                     if 'file_checksum' in file:
                      fileMd5sum = file['file_checksum']
                     #creating the header
                     indexing = {"index":{"_id": counter, "_type":"meta"}}
                     indexing = str(indexing).replace("'",'"')
                     counter += 1
                     #add all stuff to dictionary
                     try:
                        udict = {'center_name': center_name, 'project': project, 
                        'program': program, 'donor': donor, 'specimen_type': specimen_type, 'analysis_type': analysis_type, 
                        'workflow': workflow, 'download_id': download_id, 'file_type': file_type, 'title': title, 
                        'file_id':bundle_uuid_filename_to_file_uuid[download_id+'_'+title], 'experimentalStrategy': submitter_experimental_design,
                        'redwoodDonorUUID': redwoodDonorUUID, 'study':study, 'sampleId':sampleId, 'submittedSampleId':submittedSampleId,
                        'submittedDonorId': submittedDonorId, 'submittedSpecimenId':submittedSpecimenId,
                        'fileSize':fileSize, 'fileMd5sum':fileMd5sum, 'workflowVersion': workflow_version,
                        'lastModified':lastModified, 'repoDataBundleId':repoDataBundleId, 'software':software,
                        'access':args.access, 'repoBaseUrl':args.repoBaseUrl, 'repoCode':args.repoCode, 'repoCountry':args.repoCountry,
                        'repoName':args.repoName, 'repoOrg':args.repoOrg, 'repoType':args.repoType, 'specimenUUID':specimenUUID, 'metadataJson':bundle_uuid_filename_to_file_uuid[download_id+'_metadata.json'], "submitterDonorPrimarySite":submitterDonorPrimarySite
                        }
                     except Exception, e:
                        print "Error with key:", str(e)
                        try:
                            # HACK!!!! needs to be removed once the workflow is fixed
                            if title.endswith(".sortedByCoord.md.bam"):
                                udict = {'center_name': center_name, 'project': project, 'program': program, 'donor': donor,
                                         'specimen_type': specimen_type, 'analysis_type': analysis_type,
                                         'workflow': workflow, 'download_id': download_id, 'file_type': file_type,
                                         'title': title,
                                         'file_id': bundle_uuid_filename_to_file_uuid[download_id + '_sortedByCoord.md.bam'], 'experimentalStrategy': submitter_experimental_design,
                                         'redwoodDonorUUID': redwoodDonorUUID, 'study':study, 'sampleId':sampleId, 'submittedSampleId':submittedSampleId,
                                         'submittedDonorId': submittedDonorId, 'submittedSpecimenId':submittedSpecimenId,
                                         'fileSize':fileSize, 'fileMd5sum':fileMd5sum, 'workflowVersion': workflow_version,
                                         'lastModified':lastModified, 'repoDataBundleId':repoDataBundleId, 'software':software,
                                         'access':args.access, 'repoBaseUrl':args.repoBaseUrl, 'repoCode':args.repoCode, 'repoCountry':args.repoCountry,
                                         'repoName':args.repoName, 'repoOrg':args.repoOrg, 'repoType':args.repoType, 'specimenUUID':specimenUUID, 'metadataJson':bundle_uuid_filename_to_file_uuid[download_id+'_metadata.json'], "submitterDonorPrimarySite":submitterDonorPrimarySite
                                         }
                            if title.endswith(".tar.gz"):
                                udict = {'center_name': center_name, 'project': project, 'program': program, 'donor': donor,
                                         'specimen_type': specimen_type, 'analysis_type': analysis_type,
                                         'workflow': workflow, 'download_id': download_id, 'file_type': file_type,
                                         'title': title,
                                         'file_id': bundle_uuid_filename_to_file_uuid[download_id + '_tar.gz'], 'experimentalStrategy': submitter_experimental_design,
                                         'redwoodDonorUUID': redwoodDonorUUID, 'study':study, 'sampleId':sampleId, 'submittedSampleId':submittedSampleId,
                                         'submittedDonorId': submittedDonorId, 'submittedSpecimenId':submittedSpecimenId,
                                         'fileSize':fileSize, 'fileMd5sum':fileMd5sum, 'workflowVersion': workflow_version,
                                         'lastModified':lastModified, 'repoDataBundleId':repoDataBundleId, 'software':software,
                                         'access':args.access, 'repoBaseUrl':args.repoBaseUrl, 'repoCode':args.repoCode, 'repoCountry':args.repoCountry,
                                         'repoName':args.repoName, 'repoOrg':args.repoOrg, 'repoType':args.repoType, 'specimenUUID':specimenUUID, 'metadataJson':bundle_uuid_filename_to_file_uuid[download_id+'_metadata.json'], "submitterDonorPrimarySite":submitterDonorPrimarySite
                                         }
                            if title.endswith(".wiggle.bg"):
                                udict = {'center_name': center_name, 'project': project, 'program': program, 'donor': donor,
                                         'specimen_type': specimen_type, 'analysis_type': analysis_type,
                                         'workflow': workflow, 'download_id': download_id, 'file_type': file_type,
                                         'title': title,
                                         'file_id': bundle_uuid_filename_to_file_uuid[download_id + '_wiggle.bg'], 'experimentalStrategy': submitter_experimental_design,
                                         'redwoodDonorUUID': redwoodDonorUUID, 'study':study, 'sampleId':sampleId, 'submittedSampleId':submittedSampleId,
                                         'submittedDonorId': submittedDonorId, 'submittedSpecimenId':submittedSpecimenId,
                                         'fileSize':fileSize, 'fileMd5sum':fileMd5sum, 'workflowVersion': workflow_version,
                                         'lastModified':lastModified, 'repoDataBundleId':repoDataBundleId, 'software':software,
                                         'access':args.access, 'repoBaseUrl':args.repoBaseUrl, 'repoCode':args.repoCode, 'repoCountry':args.repoCountry,
                                         'repoName':args.repoName, 'repoOrg':args.repoOrg, 'repoType':args.repoType, 'specimenUUID':specimenUUID, 'metadataJson':bundle_uuid_filename_to_file_uuid[download_id+'_metadata.json'], "submitterDonorPrimarySite":submitterDonorPrimarySite
                                         }
                        except Exception, e:
                            print "Second Error with key, giving up:", str(e)
                            continue
                          
                     # adict = ast.literal_eval(json.dumps(udict))
                     adict = json.dumps(udict)
                     adict = str(adict).replace("'",'"')
                     #push header and dictionary to .jsonl
                     fb_index.write(indexing+"\n"+adict+"\n")
