#!/usr/bin/env python3
import json
import os
import sys
import argparse
import datetime
import botocore
import boto3
import yaml
import sqlite3
import uuid
import subprocess
import random
import math
from snakemake.utils import read_job_properties
import functools
print = functools.partial(print, flush=True)

def str2dt(s):
	return datetime.datetime.strptime(s, '%Y-%m-%d %H:%M:%S.%f')

def pp_table(data):
	ms = list(map(len, data[0]))
	for r in data:
		for i in range(0,len(r)):
			if r[i] is None: ms[i] = max(0, ms[i])
			elif not isinstance(r[i],str): ms[i] = len(str(r[i]))
			else: ms[i] = max(ms[i], len(r[i]))
	rf = "  ".join(map(lambda i: "{:"+str(i)+"}", ms))
	for r in data:
		print(rf.format(*(map(str,r))))

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

def boto3_all_results(function, key, **kwargs):
	r = function(**kwargs)
	rs = r[key]
	nt = r.get('NextToken', None)
	while nt is not None and nt != '':
		r = function(NextToken=nt, **kwargs)
		rs.extend(r[key])
		nt = r.get('NextToken', None)
	return rs

class Cache:
	def __init__(self, fname):
		self.db_path = fname
		self.create_db()
	def open(self):
		c = sqlite3.connect(
			self.db_path,
			timeout=10*60, # 10 minutes
			isolation_level=None # autocommit mode
		)
		c.row_factory = sqlite3.Row
		return c
	def timed_lock(self, key, delta_seconds):
		with self.open() as db:
			db.execute('BEGIN EXCLUSIVE')
			t1 = datetime.datetime.now()
			r = db.execute('select dt from timed_locks where key=?',(key,)).fetchone()
			t0 = str2dt(r[0]) if r is not None else None
			if t0 is None or (t1-t0).total_seconds() > delta_seconds:
				db.execute('insert or replace into timed_locks values(?,?)',(key,t1))
				db.execute('END')
				return True
			db.execute('END')
			return False
	def create_db(self):
		with self.open() as db:
			n, = db.execute('select count(*) from sqlite_master where type=? and name=?',('table','jobs')).fetchone()
			if n>0: return
			db.execute('create table if not exists jobs (jobid, jobname, status, instance_id, orig_jobscript, start_time, end_time, PRIMARY KEY(jobid))')
			db.execute('create table if not exists spot_prices (it, az, price, backoff, PRIMARY KEY(it,az))')
			db.execute('create table if not exists instance_types (it, cpus, mem_mb, storage_gb, PRIMARY KEY(it))')
			db.execute('create table if not exists it_features (it, key, value, PRIMARY KEY(it,key))')
			db.execute('create table if not exists timed_locks (key, dt, PRIMARY KEY(key))')

class HD:
	job_end_states = ['SUCCESS','FAILED']

	def msg(self, s, end='\n', head=True):
		h = self.pname+': ' if head else ''
		print(h+s, file=sys.stderr, end=end)

	def __init__(self):
		self.pname = sys.argv[0]
		self.parser = argparse.ArgumentParser()
		self.parser.add_argument('--config', default='hyperdrive.yaml')
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
		p3.add_argument('--ami', required=True)
		p3.add_argument('--cache', default='hyperdrive.cache')
		self.args, self.extra_args = self.parser.parse_known_args()
		self.conf = {}
		if os.path.exists(self.args.config):
			self.conf = yaml.safe_load(open(self.args.config))
			self.cache = Cache(self.conf['cache'])
		elif self.args.subcmd is not None and self.args.subcmd != 'config':
			self.msg('run "{} config" first'.format(self.pname))
			sys.exit(1)

	def create_config(self):
		cf = boto3.client('cloudformation')
		if not stack_exists(cf, self.args.stack_name):
			self.msg('stack not found')
			sys.exit(1)
		bucket, key = s3_split_path(self.args.prefix)
		s3 = boto3.client('s3')
		if not bucket_exists(s3, bucket):
			self.msg('cant access bucket: '+bucket)
			sys.exit(1)

		self.conf['cache'] = self.args.cache
		self.conf['amiId'] = self.args.ami
		self.conf['prefix'] = self.args.prefix
		self.conf['stackName'] = self.args.stack_name
		r = cf.describe_stacks(StackName=self.args.stack_name)
		output_keys = ['jobQueueUrl','logGroupName','workerProfileArn','securityGroupId','group']
		for o in r['Stacks'][0]['Outputs']:
			if o['OutputKey'] not in output_keys:
				self.msg('Stack dont match expected outputs')
				sys.exit(1)
			self.conf[o['OutputKey']] = o['OutputValue']
		yaml.dump(self.conf, open(self.args.config,'w'))

	def kill_job(self):
		ec2 = boto3.client('ec2')
		with self.cache.open() as db:
			db.execute('update jobs set status=? where jobid=?',('FAILED',self.args.jobid))
			it, = db.execute('select instance_id from jobs where jobid=?',(self.args.jobid,)).fetchone()
			ec2.terminate_instances(InstanceIds=[it])

	def clean_cache(self):
		with self.cache.open() as db:
			for jobid, st in db.execute('select jobid, status from jobs'):
				if st in HD.job_end_states:
					db.execute('delete from jobs where jobid=?',(jobid,))

	def find_instances_req(self, job_info):
		n_cpus = job_info['cpus']
		mem_mb = job_info['mem_mb']
		with self.cache.open() as db:
			c = db.execute('select it,storage_gb from instance_types where cpus>=? and mem_mb>=?',(n_cpus, mem_mb))
			l = dict(c.fetchall())
			c = db.execute('select distinct key from it_features')
			features = list(map(lambda i:i[0],c.fetchall()))
			for k in job_info['resources'].keys():
				if k not in features: continue
				c = db.execute('select it from it_features where key=? and value>=?',
				(k,job_info['resources'][k]))
				l2 = list(map(lambda i:i[0],c.fetchall()))
				l = dict(filter(lambda i: i[0] in l2, l.items()))
		return l

	def find_lowest_price(self, instance_list, storage_gb):
		self.get_spot_prices()
		ebs_gb_hour = 0.1/(24*30) # TODO
		ls = []
		with self.cache.open() as db:
			for i in instance_list.keys():
				extra_ebs = max(0,storage_gb - instance_list[i])
				for az, ec2_hour in db.execute('select az,price from spot_prices where it=? and backoff<1',(i,)):
					total_cost = float(ec2_hour) + extra_ebs*ebs_gb_hour
					ls.append({'az':az,'it':i,'cost':total_cost, 'extra_ebs': extra_ebs, 'instance_storage': instance_list[i]})
		ls = sorted(ls, key=lambda i:i['cost'])
		ls2 = list(filter(lambda i: i['cost']<=ls[0]['cost'], ls))
		return ls2

	def get_instances_info(self):
		with self.cache.open() as db:
			n, = db.execute('select count(*) from instance_types').fetchone()
			if n>0: return
		self.msg('getting instance-type data ... ', end='')

		def it_filter(it):
			if 'x86_64' not in it['ProcessorInfo']['SupportedArchitectures']: return False
			if 'SustainedClockSpeedInGhz' not in it['ProcessorInfo']: return False
			if 'spot' not in it['SupportedUsageClasses']: return False
			if 'ebs' not in it['SupportedRootDeviceTypes']: return False
			if 'GpuInfo' in it: return False
			if 'FpgaInfo' in it: return False
			if 'InferenceAcceleratorInfo' in it: return False
			if it['BareMetal']: return False
			if it['BurstablePerformanceSupported']: return False
			return True

		features_file = os.path.join(sys.path[0], 'share', 'it_features.json')
		features = json.load(open(features_file))

		ec2 = boto3.client('ec2')
		its = boto3_all_results(ec2.describe_instance_types, 'InstanceTypes')

		its = list(filter(it_filter, its))
		with self.cache.open() as db:
			for i in its:
				k = i['InstanceType']
				storage_gb = 0
				if k in features:
					for f in features[k].keys():
						db.execute('insert into it_features values(?,?,?)',(k,f,features[k][f]))
				if 'InstanceStorageInfo' in i: storage_gb = i['InstanceStorageInfo']['TotalSizeInGB']
				db.execute('insert into instance_types (it,cpus,mem_mb,storage_gb) values(?,?,?,?)',
				(k, i['VCpuInfo']['DefaultVCpus'], i['MemoryInfo']['SizeInMiB'], storage_gb))
		self.msg('done', head=False)

	def get_spot_prices(self):
		if not self.cache.timed_lock('spot_prices', 30*60): # 30 minutes
			return

		self.msg('refreshing spot prices ... ', end='')
		ec2 = boto3.client('ec2')
		with self.cache.open() as db:
			instance_list = db.execute('select distinct it from instance_types').fetchall()
			instance_list = list(map(lambda i:i[0], instance_list))

		rs = boto3_all_results(ec2.describe_spot_price_history, 'SpotPriceHistory',
			InstanceTypes=instance_list,
			MaxResults=1000,
			StartTime=datetime.datetime.utcnow(),
			EndTime=datetime.datetime.utcnow(),
			ProductDescriptions=['Linux/UNIX (Amazon VPC)']
		)
		prices = {}
		for i in rs:
			it = i['InstanceType']
			az = i['AvailabilityZone']
			if it not in prices: prices[it] = {}
			if az not in prices: prices[it][az] = {}
			if 'time' not in prices[it][az] or i['Timestamp'] > prices[it][az]['time']:
				prices[it][az] = { 'time': i['Timestamp'], 'price': i['SpotPrice'] }
		with self.cache.open() as db:
			for it in prices.keys():
				for az in prices[it].keys():
					db.execute('insert or replace into spot_prices (it,az,price,backoff) values(?,?,?,?)',
					(it,az, float(prices[it][az]['price']),0))
		self.msg('done', head=False)

	def host_userscript(self, jobid, job_info):
		host_file = os.path.join(sys.path[0], 'share', 'host.py')
		if not os.path.exists(host_file):
			self.msg('cant find host script: {}'.format(host_file))
			sys.exit(1)
		script = open(host_file).read()
		script = script.replace('<DATA>', json.dumps({
			'jobid':jobid,
			'sqs_url':self.conf['jobQueueUrl'],
			'prefix':self.conf['prefix'],
			'log_group':self.conf['logGroupName'],
			'extra_logs': job_info['log']
		}))
		return script

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
				self.msg('no log data')
				sys.exit(1)
			else:
				raise e
		for l in r['events']:
			d = datetime.datetime.fromtimestamp(round(l['timestamp']/1000))
			print(d,'|',l['message'],end='')
		print('------')
		with self.cache.open() as db:
			r = db.execute('select status from jobs where jobid=?',(self.args.jobid,)).fetchone()
			if r is not None: print('status: '+r[0])

	def print_status(self):
		# only refresh if delta time > 30 seconds
		self.check_sqs_messages(delta_seconds=30)
		self.check_instance_status(delta_seconds=30)

		data = []
		with self.cache.open() as db:
			data = db.execute('select jobid,jobname,status,start_time,end_time from jobs').fetchall()
		data = sorted(data, key=lambda k:k['start_time'])
		if len(data):
			data.insert(0, data[0].keys()) # header
			pp_table(data)

	def check_sqs_messages(self, delta_seconds=7):
		if not self.cache.timed_lock('sqs_status', delta_seconds):
			return

		sqs = boto3.client('sqs')
		r = sqs.receive_message(
			QueueUrl=self.conf['jobQueueUrl'],
			MaxNumberOfMessages=10,
			WaitTimeSeconds=2
		)
		if 'Messages' not in r: return
		with self.cache.open() as db:
			for m in r['Messages']:
				j = json.loads(m['Body'])
				r = db.execute('select status from jobs where jobid=?',(j['jobid'],)).fetchone()
				if r is not None:
					db.execute('update jobs set status=? where jobid=?',(j['status'],j['jobid']))
					sqs.delete_message(QueueUrl=self.conf['jobQueueUrl'],
						ReceiptHandle=m['ReceiptHandle'])
					if j['status'] in HD.job_end_states:
						now = datetime.datetime.now().replace(microsecond=0)
						db.execute('update jobs set end_time=? where jobid=?',(now,j['jobid']))

	def check_instance_status(self, delta_seconds=7):
		if not self.cache.timed_lock('instance_status', delta_seconds):
			return

		instance_ids = {}
		with self.cache.open() as db:
			for jobid, st, instance_id in db.execute('select jobid,status,instance_id from jobs'):
				if st != 'RUNNING': continue
				instance_ids[instance_id] = jobid

		if len(instance_ids)==0: return

		ec2 = boto3.client('ec2')
		r = boto3_all_results(ec2.describe_instances, 'Reservations',
			InstanceIds=list(instance_ids.keys())
		)

		def increase_it_backoff(instance_type, az):
			with self.cache.open() as db:
				db.execute('update spot_prices set backoff = backoff + 1 where it=? and az=?',(instance_type,az))
		def set_job_status(jobid, status):
			with self.cache.open() as db:
				db.execute('update jobs set status = ? where jobid=?',(status,jobid))

		backoff_states = ['Server.InsufficientInstanceCapacity','Server.SpotInstanceTermination']

		for i in r:
			for j in i['Instances']:
				instance_id = j['InstanceId']
				it = j['InstanceType']
				az = j['Placement']['AvailabilityZone']
				jobid = instance_ids[instance_id]
				if 'StateReason' in j:
					src = j['StateReason']['Code']
					if src == 'Client.InstanceInitiatedShutdown':
						pass # job finished, wait for sqs msg
					elif src in backoff_states: # backoff & retry
						set_job_status(jobid, 'PENDING')
						increase_it_backoff(it, az)
						with self.cache.open() as db:
							jobscript, = db.execute('select orig_jobscript from jobs where jobid=?',(jobid,)).fetchone()
						self.req_instance(jobid, jobscript) # retry job
					elif src == 'Client.UserInitiatedShutdown':
						set_job_status(jobid, 'FAILED') # terminated by ec2 api
					else: # ???
						set_job_status(jobid, 'FAILED')
						raise Exception(j)

	def get_job_status(self, jobid):
		with self.cache.open() as db:
			r = db.execute('select status from jobs where jobid=?', (self.args.jobid,)).fetchone()
			if r is None: return None
			return r[0]

	def smk_status(self):
		self.check_sqs_messages()
		self.check_instance_status()

		st = self.get_job_status(self.args.jobid)
		if st is None:
			self.msg('job not found')
			sys.exit(1)

		if st in HD.job_end_states:
			print(st.lower())
		else:
			print('running')

	def get_job_info(self, jobpath):
		job_properties = read_job_properties(jobpath)
		mem_mb = 500
		disk_gb = 0
		if 'resources' in job_properties:
			if 'mem_mb' in job_properties['resources']: mem_mb = job_properties['resources']['mem_mb']
			elif 'mem_gb' in job_properties['resources']: mem_mb = 1024*job_properties['resources']['mem_gb']
			if 'disk_gb' in job_properties['resources']: disk_gb = job_properties['resources']['disk_gb']
			elif 'disk_mb' in job_properties['resources']: disk_gb = math.ceil(job_properties['resources']['disk_mb']/1024)
		jobname = "hd-{}-{}".format(job_properties['rule'], job_properties['jobid'])
		return {
			'jobname': jobname,
			'mem_mb': mem_mb,
			'disk_gb': disk_gb,
			'cpus': job_properties.get('threads',1),
			'resources': job_properties.get('resources',{}),
			'log': job_properties.get('log',[])
		}

	def submit_job(self):
		jobid = str(uuid.uuid4())
		s3 = boto3.client('s3')
		bucket, pkey = s3_split_path(self.conf['prefix'])
		s3.upload_file(self.args.jobscript, bucket, os.path.join(pkey,'_jobs',jobid))
		self.req_instance(jobid, self.args.jobscript)
		print(jobid)

	def req_instance(self, jobid, jobscript):
		ec2 = boto3.client('ec2')
		job_info = self.get_job_info(jobscript)
		its = self.find_instances_req(job_info)
		its = self.find_lowest_price(its, job_info['disk_gb'])
		instance = random.choice(its)
		sys.stderr.write(str(instance)+'\n')
		userdata = self.host_userscript(jobid, job_info)
		tags = [
			{'Key': 'Name', 'Value': job_info['jobname'] },
			{'Key': 'HD-JobId', 'Value': jobid },
			{'Key': 'HD-Prefix', 'Value': self.conf['prefix'] },
			{'Key': 'HD-Stack', 'Value': self.conf['stackName'] }
		]
		block_devices = []
		if instance['extra_ebs'] > 0:
			block_devices.append({
				'DeviceName': '/dev/xvdz',
				'Ebs': { 'VolumeSize': instance['extra_ebs'], 'VolumeType': 'gp2' }
			})
		r = ec2.run_instances(
			MinCount=1, MaxCount=1,
			SecurityGroupIds=[self.conf['securityGroupId']],
			ImageId=self.conf['amiId'],
			InstanceType=instance['it'],
			Placement={ 'AvailabilityZone': instance['az'] },
			UserData=userdata,
			IamInstanceProfile={ 'Arn': self.conf['workerProfileArn']},
			BlockDeviceMappings=block_devices,
			InstanceMarketOptions={
				'MarketType': 'spot',
				'SpotOptions': { 'SpotInstanceType': 'one-time' }
			},
			TagSpecifications=[
				{'ResourceType': 'instance', 'Tags': tags},
				{'ResourceType': 'volume', 'Tags': tags},
			]
		)
		r = r['Instances'][0]
		#sir_id = r['SpotInstanceRequestId']
		instance_id = r['InstanceId']
		if instance_id is None or instance_id == '':
			raise Exception(r)

		now = datetime.datetime.now().replace(microsecond=0)
		with self.cache.open() as db:
			db.execute('insert or replace into jobs (jobid,jobname,status,start_time,instance_id,orig_jobscript) values(?,?,?,?,?,?)',
			(jobid, job_info['jobname'], 'RUNNING', now, instance_id,jobscript))

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
				self.get_instances_info()
				self.get_spot_prices()
			os.execvp('snakemake',['snakemake',
				'--default-remote-provider', 'S3',
				'--default-remote-prefix', self.conf['prefix'],
				'--config', 'DEFAULT_REMOTE_PREFIX='+self.conf['prefix'],
				'--no-shared-fs',
				'--use-conda',
				'--use-singularity',
				'--max-status-checks-per-second', '1',
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


