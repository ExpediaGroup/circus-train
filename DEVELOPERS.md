![Circus Train.](circus-train.png "Moving Hive data between sites.")

# Developer's Guide

## Overview

This document is a collection of notes on Circus Train which have been put together to outline what some of the main classes do and how they link together. The project is pretty large and if you haven't worked on it for a while its easy to get lost! 
These notes are meant as a helpful developer's guide into Circus Train's code and how it works, but they are not completely exhaustive of all the inner workings of the project. Do feel free to add more information or detail. 

## README.md

First and foremost, its worth having a read through the [README.md](https://github.com/HotelsDotCom/circus-train/blob/master/README.md) file. It is a pretty extensive guide containing a lot of info on the project, including how to run it and all the different configurations which can be used. 

## Classes
**Locomotive**

* This is where it all begins.
* A new `Replication` object is created using the `ReplicationFactory` and *replicate* is called on it.

**ReplicationFactory**

* Returns a `Replication` object. The type depends on whether the source table is partitioned or not, and the replication mode specified in the configuration file.

**Replication**

* Either partitioned or unpartitioned.
* There are 4 replication modes:
   * `FULL` ← default
   * `FULL_OVERWRITE`
   * `METADATA_MIRROR`
   * `METADATA_UPDATE`
* Uses a copier based on where the data is coming from and going to:
   * HDFS or S3 → HDFS, uses `DistCpCopier`
   * HDFS → S3, uses `S3MapreduceCpCopier`
   * S3 → S3,  uses `S3S3Copier`
      * Note: If you are replicating S3 → S3 cross account, *and* you want to assume a role in the target account (see `copier-options.assume-role` in `README.md`), then you must use `S3MapreduceCpCopier`.
* The data is copied over first (if the mode is `FULL` or `FULL_OVERWRITE`).
* Then the metadata of the table is updated.

## Types of replication
There are four types of replication which Circus Train can handle:

* `FULL` ← default
* `FULL_OVERWRITE`
* `METADATA_MIRROR`
* `METADATA_UPDATE`


### Full Replication
**Partitioned**

If the source table has partitions containing data then these and the corresponding data will be copied over to the replica table. After this, the metadata of the table will be updated. 

Otherwise, if the source table is partitioned but has no partitions containing data only the metadata of the table will be updated. 

**Unpartitioned** 

All data from the source is copied over to the replica table, then the metadata is updated.


### Full Overwrite Replication
This replication mode behaves in the same way as `FULL`; however, the corresponding existing replica table, if any, and its underlying data will first be deleted before being replaced with the source table and data. 

This mode is useful in the early stages of the development lifecycle if incompatible schema changes are being made frequently so one doesn't have to manually remove data that is no longer valid. 

A `DataManipulator` is used to handle the deletion of data. Determining which manipulator to use is handled in the same manner as the [Copier](#copiers), in that there is a `DataManipulatorFactoryManager` which will generate a suitable `DataManipulatorFactory` that returns a `DataManipulator` object. 

### Metadata Mirror Replication 
Only metadata will be copied (mirrored) from the source to the replica. Unlike the replication modes above, locations in the replica metadata will not be modified so your source and replica will have the same data locations.

*NOTE:* The replica table will be marked as `EXTERNAL`. This is done to prevent accidental data loss when dropping the replica. 

Example use case:  this can be used for copying someone else's metadata into your Hive Metastore without copying the data or to replicate a view. You still need to have access to the data in order to query it.

### Metadata Update Replication
This will update the metadata only for a table that was previously fully replicated.

No data will be copied but any metadata from the source will be copied and table/partition locations will keep pointing to previously replicated data.

Example use case: Update the metadata of a Hive Table (for instance to change the Serde used) without having the overhead of re-replicating all the data.

## Copiers
The copiers are the classes which perform the actual copying of the data. 

There is a `CopierFactoryManager` which determines which type of copier will be used. The `DefaultCopierFactoryManager` is an implementation of this, and has a list of `CopierFactories` auto-wired into it. Spring will find all beans which implement `CopierFactory` and pass these on to the constructor for the `DefaultCopierFactoryManager`. 

There is an optional copier option available to specify which `CopierFactory` to use, if this value is set then this copier factory class will be used. If this value is not set the `DefaultCopierFactoryManager` will check all `CopierFactories` in the list and return the first which supports replication between the SourceLocation and ReplicaLocation provided. 

The following is the order of precedence which the `CopierFactories` will be checked in to see if the replication is supported:
* `S3S3Copier`,
* `S3MapreduceCpCopier`, 
* `DistCpCopier` (i.e. this is used if all the above factories don't support the replication).

The copiers which use S3 will create clients that allow access and give permissions to perform actions on S3 buckets. In some cases an IAM role is needed (e.g. if data is being transferred across S3 accounts).

### Types of copier
**S3S3Copier**

*Replication: S3 → S3* 

This copier uses two `AwsS3Clients` - a source client and a replica client. There is an `AwsS3ClientFactory` which will create clients to perform actions on S3 buckets. 

One of these client factories is `JceksAmazonS3ClientFactory`, which creates a client with the necessary credentials. It does this using a credential provider chain, which will create (as the name states) a chain of credential providers that will be tried in order, until one is successful. One of the credentials in this chain is the `AssumeRoleCredentialProvider` which uses a role provided in the copier options to be able to replicate across S3 accounts.

The replication is handled by a `TransferManager` which uses the target S3 client and the `S3S3CopierOptions`. The `TransferManager` will be given the the source client to replicate from. 

The `S3S3CopierOptions` will take the `CopierOptions` provided and change them into more specific S3 options. For example it will have the options `s3-server-side-encryption` and `assume-role`, which are specific to S3 clients and won't be used by the other copiers. 

*Cross-account Replication*

As mentioned above, if you want to replicate from one S3 account to another S3 account you will need to make use of `roles`. 
In the config file for the replication an `assume-role` will need to be included which will be used by Circus Train to perform the replication. In order for this to work this role will need to have the following permissions:
* Read access to the source account, 
* Read and write access to the target account. 

**S3MapreduceCpCopier**

*Replication: HDFS → S3* 

Has its own `AwsS3ClientFactory` which creates a client with the necessary credentials, based on the given configuration. 


**DistCpCopier**

*Replication: HDFS or S3 → HDFS* 

This is the default copier which will be used if the two previous copiers do not support replication between the source and target.

### Roles

When trying to replicate across different S3 accounts an IAM role will need to be assumed by the client, in order to have the correct permissions to perform the replication. 
The role provided in the copier options needs to have access to the target bucket, and it needs to be able to be assumed by another role, i.e. the client needs to temporarily pretend it is this role. 
