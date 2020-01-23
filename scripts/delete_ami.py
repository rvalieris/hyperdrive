import sys
import boto3
ec2=boto3.client('ec2')

ami_id = sys.argv[1]

# get snapshot id
r = ec2.describe_images(ImageIds=[ami_id])
snapshot_id = r['Images'][0]['BlockDeviceMappings'][0]['Ebs']['SnapshotId']
print('found snapshot: '+snapshot_id)
# delete ami and snapshot
print('deregister ami: '+ami_id) 
ec2.deregister_image(ImageId=ami_id)
print('delete snapshot: '+snapshot_id)
ec2.delete_snapshot(SnapshotId=snapshot_id)
