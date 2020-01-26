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
import psutil
import inotify_simple
import multiprocessing
# always flush to keep the log going
print = functools.partial(print, flush=True)

conda_bin_path = '/opt/conda/bin'
mountdir = '/tmp'
basedir = os.path.join(mountdir,'ec2-user')
workflow_path = os.path.join(basedir, 'workflow')
jobscript_path = os.path.join(basedir, 'job.sh')
log_path = '/var/log/cloud-init-output.log'
aws = os.path.join(conda_bin_path,'aws')

data = json.loads('''<DATA>''')

def get_metadata():
	r = requests.get('http://169.254.169.254/latest/dynamic/instance-identity/document')
	return json.loads(r.text)

metadata = get_metadata()
region = metadata['region']
sqs = boto3.client('sqs', region_name=region)

def lsblk():
	p=subprocess.run(['lsblk','-b','-r','-p'],stdout=subprocess.PIPE)
	lines = p.stdout.decode().rstrip().split('\n')
	header = lines[0].split(' ')
	h = []
	for l in lines[1:]:
		l = l.split(' ')
		h2 = {}
		for i,s in enumerate(header):
			if len(l)>i: h2[s] = l[i]
			else: h2[s] = ''
		h.append(h2)
	return h

def drop_priv(pwr):
	os.setgroups([])
	os.setgid(pwr.pw_gid)
	os.setuid(pwr.pw_uid)
	os.umask(0o22)

def log_watcher():
	inotify = inotify_simple.INotify()
	cwl = boto3.client('logs', region_name=region)
	cwl.create_log_stream(logGroupName=data['log_group'], logStreamName=data['jobid'])
	wait_for_files = dict(map(lambda k: (os.path.join(workflow_path,k),1), data['extra_logs']))
	wait_for_files[log_path] = 1
	watching = {}
	kvargs = {'logGroupName':data['log_group'], 'logStreamName':data['jobid']}
	while True:
		for f in list(wait_for_files.keys()):
			if os.path.exists(f):
				wd = inotify.add_watch(f, inotify_simple.flags.MODIFY | inotify_simple.flags.ATTRIB)
				watching[wd] = open(f)
				del wait_for_files[f]
				os.utime(f) # trigger inotify now
		for e in inotify.read(timeout=5000,read_delay=1000):
			t = round(datetime.datetime.now().timestamp()*1000)
			logs = list(map(lambda l: {'timestamp': t, 'message': l}, watching[e.wd].readlines()))
			if len(logs)>0:
				kvargs['logEvents'] = logs
				r = cwl.put_log_events(**kvargs)
				kvargs['sequenceToken'] = r['nextSequenceToken']

def setup_storage():
	h = lsblk()
	root = list(filter(lambda i:i['MOUNTPOINT']=='/', h))[0]
	disks = list(filter(lambda i:i['TYPE']=='disk' and i['NAME'] not in root['NAME'], h))
	to_umount = list(filter(lambda i:i['MOUNTPOINT']!='/' and i['MOUNTPOINT'] != '', h))
	# 1. umount ephemeral
	for i in to_umount:
		subprocess.run(['umount',i['MOUNTPOINT']])
	
	# 2. create raid0
	disk_names = list(map(lambda i:i['NAME'], disks))
	device = '/dev/md0'
	if len(disk_names)>1:
		p=subprocess.Popen(['yes'],stdout=subprocess.PIPE)
		subprocess.run(['mdadm','-C','--force',device,'--level=0','-n',str(len(disks))]+disk_names,stdin=p.stdout)
	elif len(disk_names)==1:
		device = disk_names[0]
	else:
		print('hyperdrive: no scratch disk found')
		return

	# 3. mount /tmp
	subprocess.run(['mkfs.xfs','-f',device])
	subprocess.run(['mv','/tmp/ec2-user','/home/'])
	subprocess.run(['mount',device,mountdir])
	subprocess.run(['mv','/home/ec2-user',mountdir])
	subprocess.run(['chmod','777',mountdir])

# collect peak metrics while 'p' is running
def gather_metrics(p):
	m = psutil.virtual_memory()
	d = {
		'tot_mem_mb': m.total/(2**20),
		'max_mem_mb': (m.total-m.available)/(2**20),
		'tot_disk_mb': psutil.disk_usage(mountdir).total/(2**20),
		'max_disk_mb': psutil.disk_usage(mountdir).used/(2**20),
		'max_cpu_usage': sum(psutil.cpu_percent(percpu=True)),
		'n_cores': psutil.cpu_count()
	}
	while True:
		m = psutil.virtual_memory()
		d['max_mem_mb'] = max(d['max_mem_mb'], (m.total-m.available)/(2**20))
		d['max_disk_mb'] = max(d['max_disk_mb'], psutil.disk_usage(mountdir).used/(2**20))
		d['max_cpu_usage'] = max(d['max_cpu_usage'], sum(psutil.cpu_percent(percpu=True)))
		try:
			if p.wait(timeout=10) is not None: break
		except subprocess.TimeoutExpired: pass
	return d

def run():
	t0 = datetime.datetime.now()
	# setup logging
	multiprocessing.Process(target=log_watcher).start()
	# setup storage
	setup_storage()
	# copy jobscript to /root
	subprocess.run([aws,'s3','cp',os.path.join('s3://',data['prefix'],'_jobs',data['jobid']),jobscript_path])
	# sync workflow
	subprocess.run([aws,'s3','sync','--no-progress',os.path.join('s3://',data['prefix'],'_workflow'),workflow_path])
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
	p=subprocess.Popen(['bash',jobscript_path], preexec_fn=functools.partial(drop_priv, pwr), env=job_env, cwd=workflow_path)
	m = gather_metrics(p)
	print('--JOB-END--')
	print('peak memory: {:.1f}MB, {:.1f}GB, {:.1f}%'.format(m['max_mem_mb'],m['max_mem_mb']/1024,100*m['max_mem_mb']/m['tot_mem_mb']))
	print('peak disk: {:.1f}MB, {:.1f}GB, {:.1f}%'.format(m['max_disk_mb'],m['max_disk_mb']/1024,100*m['max_disk_mb']/m['tot_disk_mb']))
	print('peak cpu: {:.1f}% / {} cores'.format(m['max_cpu_usage'],m['n_cores']))
	print('total runtime: {}'.format(datetime.datetime.now()-t0))

	if p.returncode == 0:
		sqs.send_message(QueueUrl=data['sqs_url'], MessageBody=json.dumps({'jobid':data['jobid'],'status':'SUCCESS'}))
	else:
		sqs.send_message(QueueUrl=data['sqs_url'], MessageBody=json.dumps({'jobid':data['jobid'],'status':'FAILED'}))

if __name__ == '__main__':
	try:
		run()
	except Exception as e:
		print(e)
		sqs.send_message(QueueUrl=data['sqs_url'], MessageBody=json.dumps({'jobid':data['jobid'],'status':'FAILED'}))
	time.sleep(3) # give some time for the logging to finish
	subprocess.run(['sudo','poweroff'])

