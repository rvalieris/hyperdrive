import boto3
import base64

script = open("ami-script.sh").read()
userdata = base64.b64encode(script.encode()).decode('ascii')

ec2 = boto3.client('ec2')

r = ec2.request_spot_instances(
	InstanceCount=1,
	Type='one-time',
	LaunchSpecification={
		'SecurityGroupIds': ['sg-b4206bcc'],
		'ImageId': 'ami-0e2c4438b8dc5b48b',
		'InstanceType': 't3.small',
		'KeyName': 'aws-key-renan',
		'UserData': userdata,
		'IamInstanceProfile': { 'Arn': 'arn:aws:iam::577322744746:instance-profile/hd2-WorkerProfile-W1M9YHW2MS8O'},
		'BlockDeviceMappings': [{
			'DeviceName': '/dev/xvda',
			'Ebs': { 'VolumeSize': 4, 'VolumeType': 'gp2' }
		}]
	}
)
print(r)

# wait 6min ?
# create image
# tag image
# tag snapshot
# terminate instance


