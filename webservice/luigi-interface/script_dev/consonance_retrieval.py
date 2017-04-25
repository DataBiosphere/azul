import subprocess
import json

def get_consonance_status(consonance_uuid):
	cmd = ['consonance', 'status', '--job_uuid', consonance_uuid]
	status_text = subprocess.check_output(cmd)
	return json.loads(status_text)

consonance_uuid = "c9ca097b-138f-4154-aa5a-8d11c38a04ad"

print "Checking Consonance UUID:", consonance_uuid
status_json = get_consonance_status(consonance_uuid)

#for key in status_json:
#	print key
#	print status_json[key]

print consonance_uuid, "-", str(consonance_uuid == status_json['job_uuid'])
print status_json['state']
print status_json['create_timestamp']
print status_json['update_timestamp']
#if status_json['state'] == 'SUCCESS' or status_json['state'] == 'FAILED':
#	print status_json['stdout']
#	print status_json['stderr']
