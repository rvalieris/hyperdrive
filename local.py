#!/usr/bin/env python3
import json
import os
import sys
import argparse
import datetime
import botocore
import boto3
import base64
import yaml
import sqlite3
import uuid
import subprocess
import itertools
import boto3
import random
from snakemake.utils import read_job_properties

def pp_table(data):
	ms = list(map(len, data[0]))
	for r in data:
		for i in range(0,len(r)):
			if r[i] is None: r[i] = ""
			if not isinstance(r[i],str): r[i] = str(r[i])
			ms[i] = max(ms[i], len(r[i]))
	rf = "  ".join(map(lambda i: "{:"+str(i)+"}", ms))
	for r in data:
		print(rf.format(*r))

def stack_exists(cf_client,stackname):
	try:
		cf_client.describe_stacks(StackName=stackname)
		return True
	except botocore.exceptions.ClientError:
		return False

def bucket_exists(s3_client,bucket):
	try:
		s3_client.head_bucket(Bucket=bucket)
		return True
	except botocore.exceptions.ClientError:
		return False

def s3_split_path(path):
	if '/' not in path:
		return (path,'')
	else:
		return path.split('/',1)

class Cache:
	def __init__(self, fname):
		self.conn = sqlite3.connect(fname)
		self.c = self.conn.cursor()
		self.c.execute('create table if not exists kvstore (section,id,key,value, PRIMARY KEY(section,id,key))')
		self.conn.commit()
	def vput(self, section, id, key, value):
		self.c.execute('insert or replace into kvstore values(?,?,?,?)', (section, id, key, value))
		self.conn.commit()
	def dput(self, section, id, kwargs):
		for k in kwargs.keys():
			self.vput(section, id, k, kwargs[k])
	def allids(self, section):
		self.c.execute('select distinct id from kvstore where section=?',(section,))
		return list(map(lambda r:r[0], self.c.fetchall()))
	def alldata(self, section):
		self.c.execute('select id,key,value from kvstore where section=?',(section,))
		return list(self.c.fetchall())
	def select(self, query, values):
		cur = self.c.execute(query, values)
		for row in cur:
			yield row
	def vget(self, section, id, key):
		self.c.execute('select value from kvstore where section = ? and id=? and key=?', (section, id, key))
		r = self.c.fetchone()
		if r is not None: return r[0]
		else: return None
	def lget(self, section, ids, keys):
		for i in ids:
			for k in keys:
				yield self.vget(section, i, k)
	def vdel(self, section, id):
		self.c.execute('delete from kvstore where section = ? and id=?', (section,id))
		self.conn.commit()

class HD:
	job_end_states = ['SUCCESS','FAILED']
	def __init__(self):
		self.pname = sys.argv[0]
		self.parser = argparse.ArgumentParser()
		self.parser.add_argument('--config', default='conf.yaml')
		subparser = self.parser.add_subparsers(dest='subcmd')
		subparser.add_parser('snakemake', help='run snakemake')
		subparser.add_parser('smk-status').add_argument('jobid')
		subparser.add_parser('submit-job').add_argument('jobscript')
		subparser.add_parser('status',help='list jobs')
		subparser.add_parser('clean-cache', help='clean finished jobs')
		subparser.add_parser('kill', help='kill a job').add_argument('jobid')
		p2 = subparser.add_parser('log', help='print logs from a job')
		p2.add_argument('-n', '--lines', default=10, type=int, required=False)
		p2.add_argument('--head', action='store_true')
		p2.add_argument('jobid')
		p3 = subparser.add_parser('config',help='create or update hyperdrive config')
		p3.add_argument('--stack-name', required=True)
		p3.add_argument('--prefix', required=True)
		p3.add_argument('--cache', default='cache.sqlite')
		self.args, self.extra_args = self.parser.parse_known_args()
		self.conf = {}
		if os.path.exists(self.args.config):
			self.conf = yaml.safe_load(open(self.args.config))
			self.cache = Cache(self.conf['cache'])
		elif self.args.subcmd is not None and self.args.subcmd != 'config':
			print('create config file first',file=sys.stderr)
			sys.exit(1)

	def create_config(self):
		cf = boto3.client('cloudformation')
		if not stack_exists(cf, self.args.stack_name):
			print('stack not found',file=sys.stderr)
			sys.exit(1)
		bucket, key = s3_split_path(self.args.prefix)
		s3 = boto3.client('s3')
		if not bucket_exists(s3, bucket):
			print('cant access bucket: '+bucket,file=sys.stderr)
			sys.exit(1)

		self.conf['cache'] = self.args.cache
		self.conf['prefix'] = self.args.prefix
		r = cf.describe_stacks(StackName=self.args.stack_name)
		output_keys = ['jobQueueUrl','logGroupName','workerProfileArn','securityGroupId']
		for o in r['Stacks'][0]['Outputs']:
			if o['OutputKey'] not in output_keys:
				print('Stack dont match expected outputs',file=sys.stderr)
				sys.exit(1)
			self.conf[o['OutputKey']] = o['OutputValue']
		self.conf['stackName'] = self.args.stack_name
		yaml.dump(self.conf, open(self.args.config,'w'))

	def stop_instance(self, req_id, instance_id):
		ec2.cancel_spot_instance_requests(SpotInstanceRequestIds=[req_id])
		ec2.terminate_instances(InstanceIds=[instance_id])

	def kill_job(self):
		self.cache.vput(section='jobs', id=self.args.jobid, key='status', value='FAILED')
		sir_id = self.cache.vget(section='jobs', id=self.args.jobid, key='sir')
		instance_id = self.cache.vget(section='jobs', id=self.args.jobid, key='instance_id')
		self.stop_instance(sir_id, instance_id)

	def clean_cache(self):
		jobids = self.cache.allids(section='jobs')
		for i in jobids:
			if self.cache.vget(section='jobs', id=i, key='status') in HD.job_end_states:
				self.cache.vdel(section='jobs', id=i)

	def instance_type_list(self):
		# TODO: amd 
		c4 = map(lambda n: 'c4.'+n+'large', ['','x','2x','4x','8x'])
		m4 = map(lambda n: 'm4.'+n+'large', ['','x','2x','4x','10x','16x'])
		r4 = map(lambda n: 'r4.'+n+'large', ['','x','2x','4x','8x','16x'])
		c5 = map(lambda n: 'c5.'+n+'large', ['','x','2x','4x','9x','12x','18x','24x'])
		m5r5 = map(lambda s: s[0]+s[1]+'.'+s[2]+'large', itertools.product(['r5','m5'],['','a'],['','x','2x','4x','8x','12x','16x','24x']))
		others = ['m3.medium', 't3.small', 't3a.small']
		return list(itertools.chain(c4, m4, r4, c5, m5r5, others))

	def find_instances_req(self,n_cpus, mem_mb):
		t1 = datetime.datetime.now()
		t0 = self.cache.vget(section='meta', id='instance_types', key='time')
		if t0 is not None: t0 = datetime.datetime.strptime(t0, '%Y-%m-%d %H:%M:%S.%f')
		if t0 is None or (t1-t0) > datetime.timedelta(hours=1):
			self.get_instances_info()
			self.cache.vput(section='meta', id='instance_types', key='time', value=t1)

		cpus = set(map(lambda k:k[0],
			self.cache.select('select id from kvstore where section=? and key=? and value>=?',
			('instance_types','cpus',n_cpus))))
		mems = set(map(lambda k:k[0],
			self.cache.select('select id from kvstore where section=? and key=? and value>=?',
			('instance_types','mem_mb',mem_mb))))
		return list(cpus.intersection(mems))

	def find_lowest_price(self,instance_list):
		t1 = datetime.datetime.now()
		t0 = self.cache.vget(section='meta', id='spot_prices', key='time')
		if t0 is not None: t0 = datetime.datetime.strptime(t0, '%Y-%m-%d %H:%M:%S.%f')
		if t0 is None or (t1-t0) > datetime.timedelta(minutes=30):
			self.get_spot_prices()
			self.cache.vput(section='meta', id='spot_prices', key='time', value=t1)

		r = list(self.cache.select(
		'select min(value) from kvstore where section=? and id in ({})'.format(','.join(['?']*len(instance_list))),
		('spot_prices', *instance_list)))
		min_val = r[0][0]
		r = list(self.cache.select(
			'select id,key,value from kvstore where section=? and id in ({}) and value<=?'.format(
			','.join(['?']*len(instance_list))),
			('spot_prices', *instance_list, min_val)))
		return list(r)

	def get_instances_info(self):
		ec2 = boto3.client('ec2')
		instance_list = self.instance_type_list()
		r = ec2.describe_instance_types(
			InstanceTypes=instance_list
		)
		r = r['InstanceTypes']
		for i in r:
			k = i['InstanceType']
			self.cache.dput(section='instance_types', id=k, kwargs={ 'cpus': i['VCpuInfo']['DefaultVCpus'], 'mem_mb': i['MemoryInfo']['SizeInMiB']})
		return r

	def get_spot_prices(self,max_results=200):
		ec2 = boto3.client('ec2')
		instance_list = self.instance_type_list()
		r = ec2.describe_spot_price_history(
			InstanceTypes=instance_list,
			MaxResults=max_results,
			StartTime=datetime.datetime.now(),
			ProductDescriptions=['Linux/UNIX']
		)
		r = r['SpotPriceHistory']
		prices = {}
		for i in r:
			it = i['InstanceType']
			az = i['AvailabilityZone']
			if it not in prices: prices[it] = {}
			if az not in prices: prices[it][az] = {}
			if 'time' not in prices[it][az] or i['Timestamp'] > prices[it][az]['time']:
				prices[it][az] = { 'time': i['Timestamp'], 'price': i['SpotPrice'] }
		for it in prices.keys():
			for az in prices[it].keys():
				self.cache.vput(section='spot_prices', id=it, key=az, value=prices[it][az]['price'])

	def _host_userscript(self, jobid):
		script = open("host.py").read()
		script = script.replace("<JOBID>", jobid)
		script = script.replace("<SQSURL>", self.conf['jobQueueUrl'])
		script = script.replace("<PREFIX>", self.conf['prefix'])
		script = script.replace("<LOGGROUP>", self.conf['logGroupName'])
		return base64.b64encode(script.encode()).decode('ascii')

	def print_log(self):
		logs = boto3.client('logs')
		try:
			r = logs.get_log_events(
				logGroupName=self.conf['logGroupName'],
				logStreamName=self.args.jobid,
				limit=self.args.lines,
				startFromHead=self.args.head
			)
		except Exception as e:
			if e.__class__.__name__ == 'ResourceInUseException' or e.__class__.__name__ == 'ResourceNotFoundException':
				print(e)
				print('no log data', file=sys.stderr)
				sys.exit(1)
			else:
				raise e
		for l in r['events']:
			d = datetime.datetime.fromtimestamp(round(l['timestamp']/1000))
			print(d,'|',l['message'],end='')
		print('------')
		for k in ['status']:
			v = self.cache.vget(section='jobs', id=self.args.jobid, key=k)
			if v is not None: print(k+': '+v)

	def print_status(self):
		keys = ['jobid', 'jobname', 'status', 'createdAt', 'sir']
		jobids = self.cache.allids(section='jobs')
		data = []
		for k in jobids:
			l = list(self.cache.lget(section='jobs', ids=[k], keys=keys[1:]))
			data.append([k]+l)
		data = sorted(data, key=lambda k:k[3]) # createdAt
		data.insert(0, keys) # header
		pp_table(data)

	def sqs_check_messages(self):
		sqs = boto3.client('sqs')
		r = sqs.receive_message(
			QueueUrl=self.conf['jobQueueUrl'],
			MaxNumberOfMessages=10,
			WaitTimeSeconds=2
		)
		if 'Messages' in r:
			for m in r['Messages']:
				j = json.loads(m['Body'])
				st = self.cache.vget(section='jobs', id=j['jobid'], key='status')
				if st is not None:
					self.cache.vput(section='jobs', id=j['jobid'], key='status', value=j['status'])
					sqs.delete_message(QueueUrl=self.conf['jobQueueUrl'], ReceiptHandle=m['ReceiptHandle'])

	def spot_check_status(self, jobid):
		sir_id = self.cache.vget(section='jobs', id=jobid, key='sir')
		instance_id = self.cache.vget(section='jobs', id=jobid, key='instance_id')
		ec2 = boto3.client('ec2')
		if instance_id is None:
			r = ec2.describe_spot_instance_requests(
				SpotInstanceRequestIds=[sir_id]
			)
			r = r['SpotInstanceRequests'][0]
			if r['State'] == 'active':
				instance_id = r['InstanceId']
				self.cache.vput(section='jobs', id=jobid, key='instance_id', value=r['InstanceId'])
				jobname = self.cache.vget(section='jobs', id=jobid, key='jobname')
				tags = [{'Key': 'Name', 'Value': jobname },{'Key':'JobId','Value':jobid}]
				self.create_tags(instance_id, tags)
				self.cache.vput(section='jobs', id=jobid, key='status', value='RUNNING')
				return 'running'
			elif r['State'] == 'open':
				return 'running' # wait more
			elif r['State'] == 'cancelled':
				return 'failed'
			elif r['State'] == 'closed' and r['status-code'] != 'instance-terminated-by-user':
				return 'failed'
		else:
			# TODO: check instance
			return 'running'

	def smk_status(self):
		t1 = datetime.datetime.now()
		t0 = self.cache.vget(section='meta', id='status', key='time')
		if t0 is not None: t0 = datetime.datetime.strptime(t0, '%Y-%m-%d %H:%M:%S.%f')
		if t0 is None or (t1-t0).total_seconds() > 5:
			self.sqs_check_messages()
			self.cache.vput(section='meta', id='status', key='time', value=t1)

		st = self.cache.vget(section='jobs', id=self.args.jobid, key='status')
		if st in HD.job_end_states:
			print(st.lower())
		else:
			print(self.spot_check_status(self.args.jobid))

	def submit_job(self):
		jobid = str(uuid.uuid4())
		job_properties = read_job_properties(self.args.jobscript)
		job_name = "hd-{}-{}".format(job_properties['rule'], job_properties['jobid'])
		s3 = boto3.client('s3')
		bucket, pkey = s3_split_path(self.conf['prefix'])
		s3.upload_file(self.args.jobscript, bucket, os.path.join(pkey,'_jobs',jobid))

		mem_mb = 500
		if 'resources' in job_properties:
			if 'mem_mb' in job_properties['resources']: mem_mb = job_properties['resources']['mem_mb']
			elif 'mem_gb' in job_properties['resources']: mem_mb = 1024*job_properties['resources']['mem_gb']

		disk_gb = 0
		if 'resources' in job_properties:
			if 'disk_gb' in job_properties['resources']: disk_gb = job_properties['resources']['disk_gb']
			elif 'disk_mb' in job_properties['resources']: disk_gb = math.ceil(job_properties['resources']['disk_mb']/1024)
		disk_gb = disk_gb + 3 # extra space for OS

		its = self.find_instances_req(job_properties.get('threads',1), mem_mb)
		its = self.find_lowest_price(its)
		it = random.choice(its)
		sys.stderr.write(str(it)+'\n')

		userdata = self._host_userscript(jobid)
		sir_id = self.req_instance(jobid=jobid, userdata=userdata, vol_size=disk_gb, instance=it, jobname=job_name)
		self.cache.dput(section='jobs', id=jobid, kwargs={'jobname':job_name, 'status': 'QUEUED', 'createdAt':datetime.datetime.now(), 'sir': sir_id })
		print(jobid)

	def req_instance(self, jobid, userdata, vol_size, instance, jobname):
		ec2 = boto3.client('ec2')
		it = instance[0]
		az = instance[1]
		#'KeyName': self.conf['KeyName'],
		r = ec2.request_spot_instances(
			InstanceCount=1,
			Type='one-time',
			ValidUntil=datetime.datetime.utcnow()+datetime.timedelta(minutes=3),
			LaunchSpecification={
				'SecurityGroupIds': [self.conf['securityGroupId']],
				'ImageId': self.conf['AmiId'],
				'InstanceType': it,
				'Placement': { 'AvailabilityZone': az },
				'UserData': userdata,
				'IamInstanceProfile': { 'Arn': self.conf['workerProfileArn']},
				'BlockDeviceMappings': [{
					'DeviceName': '/dev/xvda',
					'Ebs': { 'VolumeSize': vol_size, 'VolumeType': 'gp2' }
				}],
			}
		)
		r = r['SpotInstanceRequests'][0]
		sir_id = r['SpotInstanceRequestId']
		return sir_id

	def create_tags(self, instance_id, tags):
		ec2 = boto3.client('ec2')
		ec2.create_tags(Resources=[instance_id],Tags=tags)
		r = ec2.describe_instances(InstanceIds=[instance_id])
		for v in r['Reservations'][0]['Instances'][0]['BlockDeviceMappings']:
			ec2.create_tags(Resources=[v['Ebs']['VolumeId']], Tags=tags)

	def main(self):
		if self.args.subcmd == 'snakemake':
			s3_workflow_path = os.path.join(self.conf['prefix'], '_workflow')
			if not ('-n' in self.extra_args or '--dry-run' in self.extra_args):
				p = subprocess.run(['aws','s3','sync',
					'--exclude','.snakemake/*',
					'--exclude','.git/*',
					'--exclude',self.args.config,
					'--exclude',self.conf['cache'],
					'--delete',
					'.', 's3://'+s3_workflow_path
				])
				if p.returncode != 0: sys.exit(p.returncode)
			os.execvp('snakemake',['snakemake',
				'--default-remote-provider', 'S3',
				'--default-remote-prefix', self.conf['prefix'],
				'--config', 'DEFAULT_REMOTE_PREFIX='+self.conf['prefix'],
				'--no-shared-fs',
				'--use-conda',
				'--use-singularity',
				'--max-status-checks-per-second', '10',
				'--cluster', self.pname+" submit-job",
				'--cluster-status', self.pname+" smk-status",
				'--jobs',str(10**6)
				]+self.extra_args
			)
	
		elif self.args.subcmd == 'smk-status':
			self.smk_status()
	
		elif self.args.subcmd == 'submit-job':
			self.submit_job()

		elif self.args.subcmd == 'status':
			self.print_status()

		elif self.args.subcmd == 'clean-cache':
			self.clean_cache()

		elif self.args.subcmd == 'kill':
			self.kill_job()

		elif self.args.subcmd == 'log':
			self.print_log()

		elif self.args.subcmd == 'config':
			self.create_config()

		else:
			self.parser.print_help()
			sys.exit(1)

if __name__ == "__main__":
	HD().main()


