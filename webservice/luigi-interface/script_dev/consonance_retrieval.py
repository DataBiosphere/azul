import subprocess
import json

def get_consonance_status(consonance_uuid):
	cmd = ['consonance', 'status', '--job_uuid', consonance_uuid, '|', 'python', '-m', 'json.tool']
	status_text = subprocess.check_output(cmd)
	return json.loads(status_text)

consonance_uuid = '550db4d9-06b5-490e-ac73-b21126c6acea'

status_json = get_consonance_status(consonance_uuid)

#for key in status_json:
#	print key
#	print status_json[key]

print consonance_uuid, "-", str(consonance_uuid == status_json['job_uuid'])
print status_json['state']
print status_json['create_timestamp']
print status_json['update_timestamp']
if status_json['state'] == 'SUCCESS' or status_json['state'] == 'FAILED':
	print status_json['stdout']
	print status_json['stderr']
