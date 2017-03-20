import json
import urllib2

from datetime import datetime, timedelta
from random import randint, choice

def get_error(server, job_id):    
    error_url = server + "fetch_error?data=%7B%22task_id%22%3A%22" + job_id + "%22%7D"
    req_data = urllib2.Request(error_url)
    response_data = urllib2.urlopen(req_data)
    text_data = response_data.read()
    json_data = json.loads(text_data)

    err_parameters = json_data["response"]
    return err_parameters["error"]

def fake_id():
    num = randint(1,10)
    for i in range(1,7):
        num = 10*num + randint(0,10)
    string = str(num)
    return "S" + string[:7]

server = "http://localhost:8082/api/"

running_url   = server + "task_list?data=%7B%22status%22%3A%22RUNNING%22%2C%22upstream_status%22%3A%22%22%2C%22search%22%3A%22%22%7D"
batch_url     = server + "task_list?data=%7B%22status%22%3A%22BATCH_RUNNING%22%2C%22upstream_status%22%3A%22%22%2C%22search%22%3A%22%22%7D"
failed_url    = server + "task_list?data=%7B%22status%22%3A%22FAILED%22%2C%22upstream_status%22%3A%22%22%2C%22search%22%3A%22%22%7D"
upfail_url    = server + "task_list?data=%7B%22status%22%3A%22PENDING%22%2C%22upstream_status%22%3A%22UPSTREAM_FAILED%22%2C%22search%22%3A%22%22%7D"
disable_url   = server + "task_list?data=%7B%22status%22%3A%22DISABLED%22%2C%22upstream_status%22%3A%22%22%2C%22search%22%3A%22%22%7D"
updisable_url = server + "task_list?data=%7B%22status%22%3A%22PENDING%22%2C%22upstream_status%22%3A%22UPSTREAM_DISABLED%22%2C%22search%22%3A%22%22%7D"
pending_url   = server + "task_list?data=%7B%22status%22%3A%22PENDING%22%2C%22upstream_status%22%3A%22%22%2C%22search%22%3A%22%22%7D"
done_url      = server + "task_list?data=%7B%22status%22%3A%22DONE%22%2C%22upstream_status%22%3A%22%22%2C%22search%22%3A%22%22%7D"

list_of_URLs = [running_url, batch_url, failed_url, upfail_url, 
                disable_url, updisable_url, pending_url, done_url]

relevant_attributes = ["status", "name", "start_time", "params"]
required_parameters = ["project", "donor_id", "sample_id", "pipeline_name"]

jobject_list = []

for URL in list_of_URLs:

    name = URL[62:]
    suffix = ""
    if "UPSTREAM" in name:
        if "FAILED" in name:
            name = "UPSTREAM_FAILED"
        else:
            name = "UPSTREAM_DISABLED"
    else:
        name = name.split("%")[0] + suffix

    error_text = None

    # Retrieve api tool dump from URL and read it into json_tools
    req = urllib2.Request(URL)
    response = urllib2.urlopen(req)
    text_tools = response.read()
    json_tools = json.loads(text_tools)

    job_list = json_tools["response"]

    if not job_list:
        # Just skip an empty response
        continue

    print job_list

    continue

    jobject = {}
    #print "\n", jobject["name"]
    jobject["error_text"] = error_text

    for job_id in job_list:

        # Get error information
        if job_list[job_id]["status"] == "FAILED":
            jobject["error_text"] = get_error(server, job_id)
            
        for attr in relevant_attributes:

            node = job_list[job_id][attr]

            if attr == "params":
                for parameter in required_parameters:
                    jobject[parameter] = job_list[job_id]["params"][parameter]
            else:
                jobject[attr] = node

        # Format time properly
        input_time = datetime.fromtimestamp(jobject['start_time']) + timedelta(hours=8)
        jobject['start_time'] = input_time.strftime('20%y-%m-%d %H:%M:%S')

        # Add unique run_id
        jobject['run_id'] = job_id[-10:]

        # Add the fake ID's
        string = fake_id()
        jobject['donor_id'] = string
        jobject['sample_id'] = string + choice(['a','b','c']) + str(randint(1,5))

        #print json.dumps(jobject)

        # Name is irritating
        del jobject['name']

    print jobject

''' 
TODO:
* How to run this script automatically?
    - How often is it called?
        : within ten minutes -> will add duplicates
        : outside of ten minutes -> might miss jobs
    - Means we'll probably have to deal with duplicates
* How to capture stdout/stderr?
* Create Elasticsearch index (outside of this repeating script)
* Push queries to Elasticsearch without jsonl
    - jsonl supported currently
* Update running/pending jobs, remove duplicates
'''
