import subprocess

def get_consonance_status(consonance_uuid):
	cmd = ['consonance', 'status', '--job_uuid', consonance_uuid, '|', 'python', '-m', 'json.tool']
	status_text = subprocess.check_output(cmd)
	return json.loads(status_text)

consonance_uuid = 'f5523781-2e1c-485d-a46b-c5b64e0f6cf8'

status_json = get_consonance_status(consonance_uuid)

print consonance_uuid, "-", str(consonance_uuid == status_json['job_uuid'])
print status_json['state']
print status_json['create_timestamp']
print status_json['update_timestamp']
print status_json['stdout']
print status_json['stderr']

# "create_timestamp": "2017-02-21T19:39:47.369+0000",
# "job_uuid": "f5523781-2e1c-485d-a46b-c5b64e0f6cf8",
# "state": "SUCCESS",
# "stderr": 
# "stdout": 
# "update_timestamp": "2017-02-21T22:48:58.214+0000",