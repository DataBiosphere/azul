import json
import urllib2

running_url   = 'http://localhost:8082/api/task_list?data=%7B%22status%22%3A%22RUNNING%22%2C%22upstream_status%22%3A%22%22%2C%22search%22%3A%22%22%7D'
batch_url     = 'http://localhost:8082/api/task_list?data=%7B%22status%22%3A%22BATCH_RUNNING%22%2C%22upstream_status%22%3A%22%22%2C%22search%22%3A%22%22%7D'
failed_url    = 'http://localhost:8082/api/task_list?data=%7B%22status%22%3A%22FAILED%22%2C%22upstream_status%22%3A%22%22%2C%22search%22%3A%22%22%7D'
upfail_url    = 'http://localhost:8082/api/task_list?data=%7B%22status%22%3A%22PENDING%22%2C%22upstream_status%22%3A%22UPSTREAM_FAILED%22%2C%22search%22%3A%22%22%7D'
disable_url   = 'http://localhost:8082/api/task_list?data=%7B%22status%22%3A%22DISABLED%22%2C%22upstream_status%22%3A%22%22%2C%22search%22%3A%22%22%7D'
updisable_url = 'http://localhost:8082/api/task_list?data=%7B%22status%22%3A%22PENDING%22%2C%22upstream_status%22%3A%22UPSTREAM_DISABLED%22%2C%22search%22%3A%22%22%7D'
pending_url   = 'http://localhost:8082/api/task_list?data=%7B%22status%22%3A%22PENDING%22%2C%22upstream_status%22%3A%22%22%2C%22search%22%3A%22%22%7D'
done_url      = 'http://localhost:8082/api/task_list?data=%7B%22status%22%3A%22DONE%22%2C%22upstream_status%22%3A%22%22%2C%22search%22%3A%22%22%7D'

list_of_URLs = [running_url, batch_url, failed_url, upfail_url, 
                disable_url, updisable_url, pending_url, done_url]

for URL in list_of_URLs:
    name = URL[62:]
    suffix = ''
    if 'UPSTREAM' in name:
        if 'FAILED' in name:
            name = 'UPSTREAM_FAILED'
        else:
            name = 'UPSTREAM_DISABLED'
    else:
        name = name.split('%')[0] + suffix
    print name

    # Retrieve api tool dump from URL and read it into json_tools
    req = urllib2.Request(URL)
    response = urllib2.urlopen(req)
    text_tools = response.read()
    json_tools = json.loads(text_tools)

    #print json_tools

    for key in json_tools:
        job_list = json_tools[key]
        if job_list:
            for thing in job_list:
                print thing
                print job_list[thing]

