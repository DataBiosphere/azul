import json
import urllib2
import subprocess

def get_consonance_status(consonance_uuid):
    cmd = ['consonance', 'status', '--job_uuid', consonance_uuid]
    status_text = subprocess.check_output(cmd)
    return json.loads(status_text)

URL = "https://dev.ucsc-cgl.org/api/v1/action/service"

# Retrieve api tool dump from URL and read it into json_tools
print "Step 1"
req = urllib2.Request(URL)
print "Step 2"
response = urllib2.urlopen(req)
print "Step 3"
text_tools = response.read()
print "Step 4"
json_tools = json.loads(text_tools)

for dictionary in json_tools:
    consonance_uuid = dictionary['consonance_job_uuid']
    if consonance_uuid != "no consonance id in test mode":
        try:
            print get_consonance_status(consonance_uuid)
        except:
	    print "Something failed."
            continue
