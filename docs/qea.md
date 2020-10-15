

### how it works ?
1. snakemake runs in "cloud" mode and sees the files in the s3 prefix as if it were local and send jobs to hyperdrive
2. hyperdrive will launch one instance per job, picking the cheapest that have the required cores, mem, disk, etc parameters of the job
3. inside the instance, a combination of ebs disk and/or instance storage will be used to meet the job requirements
4. workflow is downloaded and execution is handed off to snakemake
5. snakemake takes care of downloading software with conda/singularity, downloading input files, executing the job and uploading output files back to the s3 prefix
6. hyperdrive streams the logs in real time to cloudwatch logs
7. when the job is done the ebs disk is deleted and the instance is shutdown
8. snakemake receives confirmation that the job finished

### what happens if the spot instance is shutdown before it finishes ?
hyperdrive will launch another instance in a different instance-type/AZ, whichever combination is cheapest.

### can I limit the number of instances used at a time ?
processing 10 jobs one instance at a time costs the same as processing 10 jobs with 10 instances but the latter will finish 10 times faster, so I don't think limiting the number of instances is worth it.

### why not have multiple jobs per instance ?
multiple jobs per instance complicates the logic significantly without obvious advantages. the ec2 instance is the container that I leave aws to manage it for me.

### why not kubernetes ?
because I want to make sure I am spending as little as possible, kubernetes is very complex and I don't need this complexity for what essentially is batch job processing.

### why not AWS batch ?
the first version of hyperdrive used aws batch, but it uses docker containers and thats an extra layer of complexity that is unnecessary and makes it harder to have ebs disks per job, also with aws batch I found that it often launched bigger instances than I actually needed, and left it running for longer than needed, by doing it myself I know exactly when to launch/shutdown instances.

### why not AWS fargate ?
fargate looks promising but its just ecs under the cover, same issues with docker as with aws batch.

