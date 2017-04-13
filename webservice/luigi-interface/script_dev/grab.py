import json
import urllib2
import subprocess

def get_consonance_status(consonance_uuid):
    cmd = ['consonance', 'status', '--job_uuid', consonance_uuid]
    status_text = subprocess.check_output(cmd)
    return json.loads(status_text)

URL = "https://dev.ucsc-cgl.org/api/v1/action/service"

# Retrieve api tool dump from URL and read it into json_tools
req = urllib2.Request(URL)
response = urllib2.urlopen(req)
text_tools = response.read()
json_tools = json.loads(text_tools)

for dictionary in json_tools:
    consonance_uuid = dictionary['consonance_job_uuid']
    if consonance_uuid != "no consonance id in test mode":
        try:
            print get_consonance_status(consonance_uuid)
        except:
            continue