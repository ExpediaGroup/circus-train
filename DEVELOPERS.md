![Circus Train.](circus-train.png "Moving Hive data between sites.")

# Developer's Guide

## Overview

This document is a collection of notes on Circus Train which have been put together to outline what some of the main classes do and how they link together. The project is pretty large and if you haven't worked on it for a while its easy to get lost! 
These notes are meant as a helpful developers guide into Circus Train's code and how it works, but it not completely exhaustive of all the inner workings of the project. 

Do feel free to update these notes with more information or detail. 

## README.md

First and foremost, the first point of call for this project is the [README.md](https://github.com/HotelsDotCom/circus-train) file. It is pretty extensive with a lot of info on the project, including how to run it and all the different configurations which can be used. 

## Classes
**Locomotive**

* This is where it all begins.
* Creates a new *Replication* object using the *ReplicationFactory* and calls replicate on it.

**ReplicationFactory**

* Returns a *Replication* object, the type depends on whether the source table is partitioned or not and the replication mode specified in the config.

**Replication**

* Either partitioned or unpartitioned.
* There are 4 replication modes that the replication will be:
   * FULL ← default
   * FULL_OVERWRITE
   * METADATA_MIRROR
   * METADATA_UPDATE
* Uses a copier based on where the data is coming from and going to:
   * hdfs → hdfs, uses distcp-copier
   * hdfs → s3, uses s3-mapreduce copier
   * s3 → s3,  uses s3s3 copier
* The data is copied over first (if its not a metadata mirror/update).
* Then the metadata of the table is updated.

## Types of replication
There are four types of replication which CircusTrain can handle:

* FULL ← default
* FULL_OVERWRITE
* METADATA_MIRROR
* METADATA_UPDATE


### Full Replication
This can be partitioned or unpartitioned. 

**Partitioned**
If the source table has partitions then these and the corresponding data will be copied over to the replica table. After this, the metadata of the table will be updated. 

Otherwise, if the source table has no partitions only the metadata of the table will be updated. 

**Unpartitioned** 
All data from the source is copied over to the replica table, then the metadata is updated.


### Full Overwrite Replication
This replication mode behaves in the same was as **FULL** however any existing replica table and its underlying data will be deleted, and replaced with the source. 

This mode is useful in the early stages of lifecycle when incompatible schema changes are made. 

A *DataManipulator* is used to handle the deleting of data. Determining which manipulator to use is handled in the same manner as the Copier, in that there is a *DataManipulatorFactoryManager* which will give a suitable *DataManipulatorFactory* that will return a suitable *DataManipulator* object. 

### Metadata Mirror Replication 
Only metadata will be copied (mirrored) from the source to the replica. Replica metadata will not be modified so your source and replica will have the same data location.

*NOTE:* The replica table will be marked as `EXTERNAL`. This is done to prevent accidental data loss when dropping the replica. For example, this can be used for copying someone else's metadata into your Hive Metastore without copying the data or to replicate a view. You still need to have access to the data in order to query it.

### Metadata Update Replication
Metadata only update for a table that was previously fully replicated.

No data will be copied but any metadata from the source will be copied and table/partition locations will keep pointing to previously replicated data.

Example use case: Update the metadata of a Hive Table (for instance to change the Serde used) without having the overhead of re-replicating all the data.

## Copiers
The copiers are the classes which do the actual copying of the data. 

There is a CopierFactoryManager which determines which type of copier will be used. The DefaultCopierFactoryManager is an implementation of this, and has a list of CopierFactories auto-wired into it. Spring will find all beans which are implementations of the CopierFactory and pass these into the constructor for the DefaultCopierFactoryManager. 

There is an optional copier option available to set which CopierFactory to use, if this value is set this copier factory class will be used. If this value is not set the DefaultCopierFactoryManager will check all CopierFactories in the list and return the first which supports replication between the SourceLocation and ReplicaLocation provided. 

There is an order of precedence, which means the CopierFactories will be checked in the following order to see if the replication is supported:
* S3S3,
* S3MapreduceCpCopier, 
* and then falls down to dist cp if the above factories don't support the replication.

The copiers which use s3 will create clients that allow access and give permissions to perform actions on s3 buckets. In some cases an assume role is needed, if data is being transferred across accounts. 

### Types of copier
**S3S3Copier**

Replication: s3 → s3 

This copier uses two AwsS3Clients - a source client and a replica client. There is an *AwsS3ClientFactory* which will create the necessary clients with permissions to perform actions on s3 buckets

One of these client factories is *JceksAmazonS3ClientFactory*, which creates a client with the necessary credentials required. It does this using a credential provider chain, which will create (as the name states) a chain of credential providers which will be tried in order, until one is successful. One of the credentials in this chain is the *AssumeRoleCredentialProvider* which uses a role provided in the copier options to be able to replicate across S3 accounts.

The replication is handled by a *TransferManager* which uses the target S3 client and the *S3S3CopierOptions*. The *TransferManager* will be given the the source client to replicate from. 

The *S3S3CopierOptions* which will take the *CopierOptions* provided and change them into more specific s3 options. For example it will have the options `s3-server-side-encryption` and `assume-role`, which are specific to S3 clients and wont be used by the other copiers. 



**S3MapreduceCpCopier**

Replication: hdfs → s3 

Has its own *AwsS3ClientFactory* which creates a client with the necessary credentials, based on the given configuration. 



**DistCpCopier**

Replication: hdfs or s3 → hdfs 

This is the default copier which will be used if the two previous copiers do not support replication between the source and target.
