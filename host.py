#!/opt/conda/bin/python3
import boto3
import pwd
import os
import requests
import subprocess
import functools
import json
import datetime
import time
import inotify_simple
import multiprocessing
from snakemake.utils import read_job_properties
# always flush to keep the log going
print = functools.partial(print, flush=True)

conda_bin_path = '/opt/conda/bin'
basedir = '/home/ec2-user'
workflow_path = os.path.join(basedir, 'workflow')
jobscript_path = os.path.join(basedir, 'job.sh')
log_path = '/var/log/cloud-init-output.log'
aws = os.path.join(conda_bin_path,'aws')

jobid = '<JOBID>'
prefix = '<PREFIX>'
sqs_url = '<SQSURL>'

def get_metadata():
	r = requests.get('http://169.254.169.254/latest/dynamic/instance-identity/document')
	return json.loads(r.text)

metadata = get_metadata()
region = metadata['region']
instance_id = metadata['instanceId']

def drop_priv(pwr):
	os.setgroups([])
	os.setgid(pwr.pw_gid)
	os.setuid(pwr.pw_uid)
	os.umask(0o22)

def create_tags(kvs):
	ec2 = boto3.client('ec2')
	ec2.create_tags(Resources=[instance_id],Tags=kvs)

def log_watcher():
	cwl = boto.client('logs')
	cwl.create_log_stream(logGroupName='hd-logs', logStreamName=jobid)
	h = open(log_path)
	inotify = inotify_simple.INotify()
	wd = inotify.add_watch(log_path, inotify_simple.flags.MODIFY)
	#h.seek(0,os.SEEK_END) # goto eof
	while True:
		inotify.read(read_delay=1000)
		t = round(datetime.datetime.now().timestamp()*1000)
		logs = list(map(lambda l: {'timestamp': t, 'message': l}, h.readlines()))
		cwl.put_log_events(
			logGroupName='hd-logs',
			logStreamName=jobid,
			logEvents=logs
		)

def run():
	# setup logging
	multiprocessing.Process(target=log_watcher).start()
	# copy jobscript to /root
	subprocess.run([aws,'s3','cp',os.path.join('s3://',prefix,'_jobs',jobid),jobscript_path])

	#job_properties = read_job_properties(jobscript_path)
	#job_name = "hd-{}-{}".format(job_properties['rule'], job_properties['jobid'])

	# send message that the job is starting
	sqs = boto3.client('sqs', region_name=region)
	#sqs.send_message(QueueUrl=sqs_url, MessageBody=json.dumps({'jobid':jobid,'status':'RUNNING'}))

	# sync workflow
	subprocess.run([aws,'s3','sync','--no-progress',os.path.join('s3://',prefix,'_workflow'),workflow_path])

	# set permissions on workflow
	pwr = pwd.getpwnam('ec2-user')
	subprocess.run(['chown','-R',"{}:{}".format(pwr.pw_uid,pwr.pw_gid),basedir])

	# start job
	job_env = os.environ.copy()
	job_env['LC_ALL'] = 'C'
	job_env['LANG'] = 'C'
	job_env['HOME'] = basedir
	job_env['PATH'] = conda_bin_path + os.pathsep + job_env['PATH']
	print('--JOB-START--')
	#p = subprocess.run(['su','-c','cd workflow;source '+jobscript_path,'--login','ec2-user'])
	p=subprocess.run(['bash',jobscript_path], preexec_fn=functools.partial(drop_priv, pwr), env=job_env, cwd=workflow_path)
	print('--JOB-END--')

	if p.returncode == 0:
		sqs.send_message(QueueUrl=sqs_url, MessageBody=json.dumps({'jobid':jobid,'status':'SUCCESS'}))
	else:
		sqs.send_message(QueueUrl=sqs_url, MessageBody=json.dumps({'jobid':jobid,'status':'FAILED'}))

if __name__ == '__main__':
	try:
		run()
	except:
		sqs = boto3.client('sqs', region_name=region)
		sqs.send_message(QueueUrl=sqs_url, MessageBody=json.dumps({'jobid':jobid,'status':'FAILED'}))
	subprocess.run([aws,'s3','cp',log_path,os.path.join('s3://',prefix,'_logs',jobid)])
	time.sleep(2) # give some time for the logging to finish
	subprocess.run(['sudo','poweroff'])

