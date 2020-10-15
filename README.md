
# hyperdrive

run [snakemake](https://snakemake.readthedocs.io/) workflows on AWS.

## features

- uses spot instances
- dynamically picks instance-types according to cpus, memory, disk, price and other features
- uses instance storage when possible to reduce costs
- does not need a shared filesystem, all data is stored on S3
- stream all logs in real time to cloudwatch logs
- distributes instances across all AZs of a region

## getting started

read the [guide](docs/guide.md) and the [q&a](docs/qea.md).

## disclaimer

I wrote hyperdrive to scratch my own itch, it still has many rough edges, do not use it unless you understand how it works.

