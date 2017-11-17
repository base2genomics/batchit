batchit is a collection of utilities for working with AWS batch.


usage
=====

```
batchit Version: $version

ddv        : detach and delete a volume by id
ebsmount   : create and mount an EBS volume from an EC2 instance
efsmount   : EFS drive from an EC2 instance
localmount : RAID and mount local storage
submit     : run a batch command


```

submit
------

example:

```
batchit submit --image worker:latest --role worker-role --queue big-queue --jobname my-work --ebs /mnt/my-ebs:500:st1:ext4 some.sh
```

where the `image` must be present in your elastic container registry, and the role and queue in their respective places
(IAM, batch respectively). In this example `some.sh` contains the commands to be run. It will have access to a 500GB
`st1` volume created with ext4 and mounted to `/mnt/my-ebs`.

The volume will be cleaned up automatically when the container exits.


ebsmount
--------

create, attach, format, and mount an EBS volume of the specified size and type to the specified mount-point.
If `-n` is greater than 1, then it will automatically RAID0 (performance, not reliability) the drives.

```
Usage: batchit [--size SIZE] --mountpoint MOUNTPOINT [--volumetype VOLUMETYPE] [--fstype FSTYPE] [--iops IOPS] [--n N] [--keep]

Options:
  --size SIZE, -s SIZE   size in GB of desired EBS volume [default: 200]
  --mountpoint MOUNTPOINT, -m MOUNTPOINT
                         directory on which to mount the EBS volume
  --volumetype VOLUMETYPE, -v VOLUMETYPE
                         desired volume type; gp2 for General Purpose SSD; io1 for Provisioned IOPS SSD; st1 for Throughput Optimized HDD; sc1 for HDD or Magnetic volumes; standard for infrequent [default: gp2]
  --fstype FSTYPE, -t FSTYPE
                         file system type to create (argument must be accepted by mkfs) [default: ext4]
  --iops IOPS, -i IOPS   Provisioned IOPS. Only valid for volume type io1. Range is 100 to 20000 and <= 50\*size of volume.
  --n N, -n N            number of volumes to request. These will be RAID0'd into a single volume for better write speed and available as a single drive at the specified mount point. [default: 1]
  --keep, -k             dont delete the volume(s) on termination (default is to delete)
  --help, -h             display this help and exit
  --version              display version and exit

```

efsmount
--------

This is a trivial wrapper around mounting an EFS volume.

```
Usage: batchit [--mountoptions MOUNTOPTIONS] EFS MOUNTPOINT

Positional arguments:
  EFS                    efs DNS and mount path (e.g.fs-XXXXXX.efs.us-east-1.amazonaws.com:/mnt/efs/)
  MOUNTPOINT             local directory on which to mount the EBS volume

Options:
  --mountoptions MOUNTOPTIONS, -o MOUNTOPTIONS
                         options to send to mount command
  --help, -h             display this help and exit
```
