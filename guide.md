
# Getting started guide

Before your workflow can be executed some preparation must be done:

* upload input data
* create the stack
* prepare the workflow

## Pick a prefix an upload your input data

Pick a new prefix: a prefix can be a S3 bucket or a subdirectory
of a S3 bucket, preferably empty to avoid conflicts with
existing files.
All input/output files of the workflow will be relative to this prefix.

There will be 3 kinds of files stored on this prefix:
  * input files
  * workflow directory
  * output files

Input files is any file required, but not created, by the workflow,
examples are `.fastq`, genomes, annotations.
Upload your input data to somewhere inside the prefix, such as
`<prefix>/input`.

Workflow directory is the working directory where you run snakemake,
it includes the `Snakefile`, configs, extra rule files, envs.yaml, scripts.
The workflow directory will be sync'ed automatically to
`<prefix>/_workflow`, this is for convenience and to always keep
the workflow current.
This directory will be downloaded by all jobs, so don't put big files here.

## Create the stack

Go to cloudformation on the aws console and create a stack with the
included template.

The template automates the creation of aws elements required for hyperdrive
to work, creating the stack has no cost, only when we start submitting jobs
the instances will be created.

The stack only needs to be created once per AWS region, and can be used many times,
even by multiple workflows at the same time, just make sure each workflow
is running on its own prefix.

## Create the AMI

A custom instance image (AMI) is required to run the jobs.
a [packer](https://packer.io) template is included to automatically
build the AMI:
`cd share; packer build packer-ami-template.json`

after the build is complete the AMI id will be printed on screen.

The AMI only needs to be built once per AWS region.

## Create the hyperdrive config

On your snakemake workflow directory, create a config file:
`hyperdrive config --stack-name <stack> --prefix <prefix> --ami <ami_id>`

## Prepare the workflow

Arguably the hardest part, is preparing the workflow to run on the cloud
environment, specially if you have an existing workflow, it probably contains
assumptions about file paths, number of cores, memory, software installed, etc,
all these assumptions need to be explicitly declared.

* use `hyperdrive snakemake --dry-run` to test if the rules were formed correctly.

* inputs: all files inside the prefix can be used as if it were local files,
example: a file in `<prefix>/input/data.txt` can be used on a rule as:
`input: "input/data.txt"` same for outputs.

* there is no shared filesystem, this means all rules need to explicitly
define all inputs and outputs required by the job to run, so for example,
an alignment job need as input not only the sequence read files but also the
genome fasta and all index files required by the aligner.
when the job starts all _declared_ input files will be downloaded to the instance,
and when the job ends all _declared_ output files will be uploaded to the prefix.
any other _undeclared_ file will be lost.

* Define cpus, memory and disk requirements:
  * use `threads` to allocate vcpus/cores/threads.
  * use `resources.mem_gb` or `mem_mb` to allocate memory,
  * use `resources.disk_gb` or `disk_mb` to allocate disk space,
    this space needs to accomodate all input+output+temporary files of the job.
  * `resources` can be a function of the inputs:
    `resources: disk_gb=lambda wc, input: round(0.5+2*input.size/1e6)`
  * use `--default-resources` to provide defaults for all rules

* files outside the prefix or in other buckets can be used as inputs
with `S3.remote("<bucket>/path/to/file")`

* local rules run on the local machine, and input files will be downloaded.
use `S3.remote()` on `rule all` to avoid downloading final results.

* `group` jobs execute together, use it to minimize overhead of short lived jobs
the `resources` of all rules in a group will be combined with `max()`

* use `conda` or `singularity` to deploy software.
the instance only have very basic linux tools.

* avoid `run:`, it will not be run inside the conda/singularity env

* `temp()`, `directory()`: does not work
  * jobs that generate many output files can use `tar` to bundle a directory
into a single output file.
  * use a common `tmp` path for temporary output files to make it easier to delete later

* the prefix can be accessed with `config['DEFAULT_REMOTE_PREFIX']`
  * can be used on the workflow to avoid hardcoding the prefix


## Run workflow

on the workflow directory:
`hyperdrive snakemake [[extra arguments for snakemake]]`

This command will sync the workflow dir, and run snakemake
with all the required options to run in "cloud" mode.

You can add extra snakemake arguments as usual, such as `--dry-run`

as snakemake submits jobs, instances will be started to process
the queue and terminated when not needed anymore.

`hyperdrive status` shows status of submitted jobs
`hyperdrive log <jobid>` prints the log of a job as it runs
`hyperdrive kill <jobid>` to terminate a job if something goes wrong.

## Tags

All instances and volumes used are tagged with the following tags:
* `Name`, a unique per workflow name using snakemake rule name and a number
* `hyperdrive.jobid`, the jobid running on the instance
* `hyperdrive.prefix`, the S3 prefix being used
* `hyperdrive.stack`, the name of the stack being used
* `hyperdrive.rule`, the snakemake rule that created the job
* `hyperdrive.wildcards.<x>`, wildcards of the job

## Tips & Gotchas

* aws have quotas/limits on how much of a resource you can use, you might
need to contact support to increase these limits if you want to run big workflows.

* Set a retention policy on cloudwatch logs, by default the logs are kept for a month.

* Track workflow costs by activating cost allocation tags on the [aws billing page](https://console.aws.amazon.com/billing/home),
however note that S3 usage can only be tracked on the bucket level, not prefixes.

* Check if you have a default subnet on all AZs of your region, no instances will be launched on a AZ without a subnet.

