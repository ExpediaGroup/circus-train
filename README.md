![Circus Train.](circus-train.png "Moving Hive data between sites.")

# Start using
You can obtain Circus Train from Maven Central:

[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.hotels/circus-train/badge.svg?subject=com.hotels:circus-train)](https://maven-badges.herokuapp.com/maven-central/com.hotels/circus-train) [![Build Status](https://travis-ci.org/HotelsDotCom/circus-train.svg?branch=master)](https://travis-ci.org/HotelsDotCom/circus-train) [![Coverage Status](https://coveralls.io/repos/github/HotelsDotCom/circus-train/badge.svg?branch=master)](https://coveralls.io/github/HotelsDotCom/circus-train?branch=master) ![GitHub license](https://img.shields.io/github/license/HotelsDotCom/circus-train.svg)

## Overview
Circus Train replicates Hive tables between clusters on request. It replicates both the table's data and metadata. Unlike many other solutions it has a light touch, requiring no direct integration with Hive's core services. However, it is not event driven and does not know how tables differ between sites; it merely responds to requests to copy (meta-)data. It can copy either entire unpartitioned tables or user defined sets of partitions on partitioned tables. Circus Train employs snapshot isolation to minimise the impact of changing data at the source, and to allow consumers of data in the replica cluster to operate independently of ongoing replication tasks.
A more detailed overview and the background of this project can be found in this blog post: [Replicating big datasets in the cloud](https://medium.com/hotels-com-technology/replicating-big-datasets-in-the-cloud-c0db388f6ba2).

## Install
Download the version to use from [Maven Central](http://mvnrepository.com/artifact/com.hotels/circus-train/) and uncompress it in a directory of your choosing.

## General operation
Below is a high level summary of the steps that Circus Train performs during the course of a typical run (different configuration might change this).

1. Fetch the configured entities to replicate from the source Hive metastore.
2. Extract the data locations for these entities.
3. Create an HDFS snapshot of the source data if possible - if the source data is in HDFS.
4. Create a new and unique destination folder on the replica.
5. Run a distributed copy operation on the local cluster to copy the data from the source snapshot to the new replica folder. For S3 to S3 operation the copy is delegated to the AWS backend.
6. Delete the snapshot on the source - if a snapshot was created.
7. Transform the source Hive entities to make them relevant to the destination:
    1. Set various table/partition parameters with replication information.
    2. Modify the data storage locations to those in the new replica folder.
8. Create any entities in the replica metastore if they don't already exist, otherwise alter the existing entities.
9. Schedule the deletion of any replica data that has now been replaced with new data.

## Usage
To run Circus Train you just need to execute the `bin/circus-train.sh` script in the installation directory and pass the configuration file which includes the replication configurations: 

        $CIRCUS_TRAIN_HOME/bin/circus-train.sh --config=/path/to/config/file.yml

### EMR
If you are planning to run Circus Train on EMR you will need to set up the EMR classpath by exporting the following environment variables before calling the `bin/circus-train.sh` script:

        export HCAT_LIB=/usr/lib/hive-hcatalog/share/hcatalog/
        export HIVE_LIB=/usr/lib/hive/lib/

Note that the paths above are correct as of when this document was last updated but may differ across EMR versions, refer to the [EMR release guide](http://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-release-components.html) for more up to date information if necessary.

### Exit codes
Circus Train returns the following exit codes:

* _0_ if all configured replications succeeded.
* _-1_ if all the configured replications failed.
* _-2_ if at least one of the configured replications succeeded but one or more of the other replications failed.

## Configuring replication and housekeeping
By default when Circus Train runs it will first perform all configured replications and then it will perform an additional "housekeeping" step where it removes any dereferenced data in the replica file system. Depending on the nature of your replications this housekeeping might take a while to run. It is possible to schedule a separate  (e.g. daily) run to perform this and configure your other Circus Train runs to perform replication only. The above two scripts support this via a `--modules` parameter that can be set to one of the following values:

* `replication, housekeeping`: This is the default value where Circus Train will first perform any configured table replications followed by the housekeeping process.
* `replication`: Circus Train will perform table replications *only*. The housekeeping process will not be executed.
* `housekeeping`: Circus Train will perform housekeeping *only*. Table replications will not be executed.

For example, the following command sequence will execute Circus Train twice, first performing replication and then housekeeping:

        $CIRCUS_TRAIN_HOME/bin/circus-train.sh --config=/path/to/config/file.yml --modules=replication
        $CIRCUS_TRAIN_HOME/bin/circus-train.sh --config=/path/to/config/file.yml --modules=housekeeping

The preferred way to execute the housekeeping process is by using the `housekeeping.sh` script which is described below.

### Run housekeeping only
If you want to schedule housekeeping as a separate process then you should use the following script:

        $CIRCUS_TRAIN_HOME/bin/housekeeping.sh --config=/path/to/config/file.yml

## Configuration
Circus Train uses [Spring Boot](http://projects.spring.io/spring-boot/) for configuration so you are free to use any of the [many configuration strategies](https://docs.spring.io/spring-boot/docs/current/reference/html/boot-features-external-config.html) supported by this framework to configure your Circus Train instance.

### Example configurations
The examples below all demonstrate configuration using [YAML](http://yaml.org/) and provide fragments covering the most common use cases that should be useful as a basis for building your own configuration. A full configuration reference is provided in the following sections.

#### Configuring source and replica
The YAML fragment below shows some common options for setting up the base source (where data is coming from) and replica (where data is going to).

        source-catalog:
          name: on-premises-cluster
          disable-snapshots: false
        replica-catalog:
          name: aws-data-warehouse
          hive-metastore-uris: thrift://emr-master.compute.amazonaws.com:9083
          metastore-tunnel:
            route: ec2-user@bastion-host -> hadoop@emr-master
            private-keys: /home/user/.ssh/bastion-key-pair.pem,/home/user/.ssh/emr-key-pair.pem
            known-hosts: /home/user/.ssh/known_hosts
        security:
          credential-provider: jceks://hdfs[@namenode:port]/hcom/etl/<team>/conf/aws-<account>.jceks
        copier-options:
        table-replications:
          - ...

#### Replicate an unpartitioned table
The YAML fragment below shows some common options for setting up the replication of an unpartitioned table where the *entire* table will be replicated on each run.

        table-replications:
          - source-table: 
              database-name: test_database
              table-name: test_unpartitioned_table
            replica-table:
              database-name: test_database
              table-name: test_unpartitioned_table
              table-location: s3://bucket/table/path

#### Replicate a partitioned table (with an errant `table.location`)
The YAML fragment below shows some common options for setting up the replication of a partitioned table where the table location in Hive is incorrect (and is thus being overridden to be the actual base location on HDFS) and also only the last 7 days worth of data is replicated on each run.

        table-replications:
          - source-table:
              database-name: test_database
              table-name: test_partitioned_table
              table-location: /source/my_folder/test_database/test_table_partitioned
              partition-filter: "local_date >= #{#nowEuropeLondon().minusDays(7).toString('yyyy-MM-dd')}"
              partition-limit: 3
            replica-table:
              table-location: s3://bucket/table/path

#### Replicate a view
The YAML fragment below shows some common options for setting up the replication of a view which uses a table that has already been replicated to the target metastore.

        table-replications:
          - replication-mode: METADATA_MIRROR
            table-mappings:
              test_database.test_unpartitioned_table: replica_db.replica_test_unpartitioned_table
            source-table: 
              database-name: test_database
              table-name: test_view
            replica-table:
              database-name: replica_db
              table-name: replica_test_view

### Replication configuration reference
The table below describes all the available configuration values for Circus Train.

|Property|Required|Description|
|:----|:----:|:----|
|`source-catalog.name`|Yes|A name for the source catalog for events and logging.|
|`source-catalog.disable-snapshots`|No|Controls whether HDFS snapshots will be used on the source data locations when replicating data. If the source location is HDFS and has been made "snapshottable" we recommend setting this to `false`. Default is `false` it will log a warning if the folder is not "snapshottable" and continue.|
|`source-catalog.hive-metastore-uris`|No|Fully qualified URI of the source cluster's Hive metastore Thrift service. If not specified values are taken from the hive-site.xml on the Hadoop classpath of the machine that's running Circus Train. This property mimics the Hive property "hive.metastore.uris" and allows multiple comma separated URIs.|
|`source-catalog.site-xml`|No|A list of Hadoop configuration XML files to add to the configuration for the source.|
|`source-catalog.configuration-properties`|No|A list of `key: value` pairs to add to the Hadoop configuration for the source.|
|`source-catalog.metastore-tunnel.*`|No|See metastore tunnel configuration values below.|
|`replica-catalog.name`|Yes|A name for the replica catalog for events and logging.|
|`replica-catalog.hive-metastore-uris`|Yes|Fully qualified URI of the replica cluster's Hive metastore Thrift service. On AWS this usually comprises of the EMR master node public hostname and metastore thrift port. This property mimics the Hive property "hive.metastore.uris" and allows multiple comma separated URIs.|
|`replica-catalog.site-xml`|No|A list of Hadoop configuration XML files to add to the configuration for the replica.|
|`replica-catalog.configuration-properties`|No|A list of `key:value` pairs to add to the Hadoop configuration for the replica.|
|`replica-catalog.metastore-tunnel.*`|No|See metastore tunnel configuration values below.|
|`security.credential-provider`|No|URL(s) to the Java Keystore Hadoop Credential Provider(s) that contain the S3 access.key and secret.key for the source or destination S3 buckets.|
|`copier-options`|No|Globally applied `Copier` options. See [Copier options](#copier-options) for details.|
|`table-replications[n].source-table.database-name`|Yes|The name of the database in which the table you wish to replicate is located.|
|`table-replications[n].source-table.table-name`|Yes|The name of the table which you wish to replicate.|
|`table-replications[n].source-table.table-location`|No|The base path of the table (fully qualified URI). Required only if your table is partitioned, external, and has its location set to a path different to that of the base path of its partitions.|
|`table-replications[n].source-table.partition-filter`|No|A filter to select which partitions to replicate. Used for partitioned tables only. See [Partition filters](#partition-filters) for more information.|
|`table-replications[n].source-table.generate-partition-filter`|No|Set to `true` to enable the "Hive Diff" feature. See [Partition filter generation](#partition-filter-generation) for details. Default is `false`. If `true` the `table-replications[n].source-table.partition-filter` will be ignored and instead a generated filter will be used.|
|`table-replications[n].source-table.partition-limit`|No|A limit on the number of partitions that will be replicated. Used for partitioned tables only.|
|`table-replications[n].replica-table.table-location`|Yes|The base path of the replica table (fully qualified URI).|
|`table-replications[n].replica-table.database-name`|No|The name of the destination database in which to replicate the table. Defaults to source database name.|
|`table-replications[n].replica-table.table-name`|No|The name of the table at the destination. Defaults to source table name.|
|`table-replications[n].copier-options`|No|Table specific `Copier` options which override any global options. See [Copier options](#copier-options) for details.|
|`table-replications[n].replication-mode`|No|Table replication mode. See [Replication Mode](#replication-mode) for more information. Defaults to `FULL`.|
|`table-replications[n].replication-strategy`|No|Table replication strategy. See [Replication Strategy](#replication-strategy) for more information. Defaults to `UPSERT`.|
|`table-replications[n].transform-options`|No|Map of optional options that can be used to set configuration for a custom transformation per table replication.|
|`table-replications[n].table-mappings`|No|Only used by view replications. This is a map of source tables used by the view and their equivalent name in the replica metastore.|

The table below describes the tunnel configuration values for source/replica catalog:

| Property | Required | Description |
|:----|:----:|:----|
| `*.metastore-tunnel.route` | No |A SSH tunnel can be used to connect to source/replica metastores. The tunnel may consist of one or more hops which must be declared in this property. See [Configuring a SSH tunnel](#configuring-a-ssh-tunnel) for details. |
| `*.metastore-tunnel.private-keys` | No |A comma-separated list of paths to any SSH keys required in order to set up the SSH tunnel. |
| `*.metastore-tunnel.known-hosts` | No |Path to a known hosts file. |
| `*.metastore-tunnel.port` | No |The port on which SSH runs on the replica master node. Default is `22`.|
| `*.metastore-tunnel.localhost` | No | The address on which to bind the local end of the tunnel. Default is '`localhost`'. |
| `*.metastore-tunnel.timeout` | No | The SSH session timeout in milliseconds, `0` means no timeout. Default is `60000` milliseconds, i.e. 1 minute. |
| `*.metastore-tunnel.strict-host-key-checking` | No | Whether the SSH tunnel should be created with strict host key checking. Can be set to `yes` or `no`. The default is `yes`. |

#### Partition filters
Control over which data and metadata Circus Train replicates can be achieved through the use of partition filters which return a list of partitions matching the filter and only data in these partitions will be replicated. This is useful if you have a very large table where only a few partitions change every day. Instead of replicating the entire table on each run of Circus Train you could create a partition filter which matches the changed partitions and only these would then be replicated.

Partition filters can be specified using the syntax described in [HIVE-1609](https://issues.apache.org/jira/browse/HIVE-1609) which states:

> The filter supports "=", "!=", ">", "<", ">=", "<=" and "LIKE" operations on partition keys of type string. "AND" and "OR" logical operations are supported in the filter. So for example, for a table having partition keys country and state, the filter can be 'country = "USA" AND (state = "CA" OR state = "AZ")'

In particular notice that it is possible to nest sub-expressions within parentheses.

Encoding a constant literal expression into a replication configuration is not especially useful unless you always want to copy the same partitions each time. Therefore it is possible to introduce dynamically evaluated elements using Spring SpELs. We refer you to the [SpEL documentation](https://docs.spring.io/spring/docs/current/spring-framework-reference/core.html#expressions) for the specific syntax and rules, however they quite simply allow you to embed Java language fragments into the filter which are dynamically evaluated to a string value. As most partition filters are date based, Circus Train includes the [Joda Time API](http://www.joda.org/joda-time/) on its classpath and we've also statically imported some convenience methods into the SpEL root context:

* `DateTime nowUtc()`
* `DateTime nowEuropeLondon()`
* `DateTime nowAmericaLosAngeles()`
* `DateTime nowInZone(String zone)`
* `DateTime nowInZone(DateTimeZone zone)`
* `String zeroPadLeft(int value, int width)`
* `String zeroPadLeft(String value, int width)`

It's also worth stating again that this is Java, encoded in a SpEL, contained within YAML. Therefore due consideration is needed with respect to the quoting, commenting, and escaping rules of all of these syntaxes. In particular bear in mind that the character `#` is heavily overloaded. Its uses include (but are not limited to): initiating a SpEL, beginning a YAML comment, and referencing the root context within a SpEL. We have also found some sensitivity to the space character; `#{#nowUtc()...` is parsed correctly whereas `#{ #nowUtc()...` fails. We believe this occurs because the YAML parser is keen to interpret the character sequence of a space followed by a hash as a comment, wherever it may be.

An example partition filter entry to match partitions that have been added in the last three days for a data set partitioned by a field called "local_date" might look something like:

    partition-filter: local_date >= '#{#nowEuropeLondon().minusDays(3).toString("yyyy-MM-dd")}'

By default characters in the property value are treated simply as string literals and this is the case with the character sequence: `local_date >= `. In this example we use single quotes to ensure that the first `#{` is interpreted as a SpEL prefix and not a YAML comment. The second `#` is a reference to the SpEL root context from which we can access the `nowEuropeLondon` convenience method. The rest is just regular Joda API in Java. Once evaluated this expression yields a result such as:

    local_date >= '2016-05-13'

You can use the `check-filters.sh` tool in `circus-train-tool` to test your expressions against a real metastore without actually invoking any table replications. This is a read-only operation and will not modify any data or metadata:

    <circus-train-tool-dir>/bin/check-filters.sh --config=your.yml

##### Partition filter generation
This is also known as "Hive Diff".

There are certain cases where one may not be able to create a partition filter as described above but there is still a need to limit the amount of data that is transferred with every run to be only a "delta" of what has changed since the last run (as opposed to replicating the entire table). For example one might have a very large table that isn't partitioned by a date or the partitions themselves don't change but the data within them does.

For these situations Circus Train provides the option to automatically generate the filter by comparing and then computing a difference between source and replica - this is referred to as its "Hive Diff" feature. Circus Train will compare the relevant tables and only replicate partitions that are missing or changed in the replica. It does this by looking at table meta data as well as the actual files on both the replica and source. This is a potentially expensive operation as Circus Train needs to fetch all partitions from the metastore and possibly make many calls to the file system so should be used with care. To enable auto generation set the following property: `table-replications[n].source-table.generate-partition-filter: true`

The properties below are relevant for the filter generation:

|Property|Required|Description|
|----|----|----|
|`table-replications[n].source-table.generate-partition-filter`|No|Set this to `true` to automatically generate partition filters based on changed data/metadata. The default is `false`.|
|`table-replications[n].partition-iterator-batch-size`|No|Number of partition objects that will be stored in memory from the source table. The default is `1000`.|
|`table-replications[n].partition-fetcher-buffer-size`|No|Number of partition objects that will be stored in memory from the replica table. The default is `1000`.|
|`table-replications[n].source-table.partition-limit`|No|Number of partitions that will be replicated. Used for partitioned tables only. When used in conjunction with a generated partition filter this also limits the generated partitions.|

#### Replication Mode
Circus Train provides configurable replication modes which can be used to control whether to replicate data and metadata or just metadata (and if just metadata *how* this is replicated). This is configured using via the `table-replication` setting which can have an optional property `replication-mode`.

It has the following options:

* `FULL`: Default behaviour, all metadata and data of the source table will be copied to the destination table and destination location. Metadata will be updated to point to the replicated data.
* `METADATA_MIRROR`: Only metadata will be copied (mirrored) from the source to the replica. Replica metadata will not be modified so your source and replica will have the same data location. NOTE: The replica table will be marked as `EXTERNAL`. This is done to prevent accidental data loss when dropping the replica. For example, this can be used for copying someone else's metadata into your Hive Metastore without copying the data or to replicate a view. You still need to have access to the data in order to query it.
* `METADATA_UPDATE`: Metadata only update for a table that was previously fully replicated. No data will be copied but any metadata from the source will be copied and table/partition locations will keep pointing to previously replicated data. Example use case: Update the metadata of a Hive Table (for instance to change the Serde used) without having the overhead of re-replicating all the data.

***Restrictions***:
* `METADATA_MIRROR` the only supported replication mode for Hive views.
* You cannot `METADATA_MIRROR` a previously fully replicated table (use `METADATA_UPDATE` if you want to do that). Similarly you cannot go from `METADATA_MIRROR` to `FULL`/`METADATA_UPDATE` as you then might end up with a table that points both to original and replicated data. Replicate to new a table if `METADATA_UPDATE` doesn't suffice for your use case. Circus Train will fail with an exception if any of these restrictions are not met.

#### Replication Strategy
Circus Train provides configurable replication strategies which can be used to control whether "destructive actions" like dropping tables or partitions should be propagated to the replica table. 

It has the following options:

* `UPSERT`: Default behaviour, data is only added to the replica. If the source tables or partitions are deleted these changes are *not* propagated to the replica.
* `PROPAGATE_DELETES`: Like UPSERT but Circus Train will also propagate deletes from the source to the replica. If a source table is deleted then the replica table will also be deleted. Similarly if there are any partitions in the source table that have been deleted they will also be deleted from the replica table. The deletes apply to both metadata and the underlying data (which is scheduled for deletion using Circus Train's Housekeeping mechanism).
 
#### Copier options
Circus Train uses highly configurable means to copy the actual data between clusters. Control over this is provided by "copier options" which allow fine grained configuration of the copier processes. The default values should suffice for most use cases but the below sections describe the various options available as they might be useful in certain situations.

##### DistCp copier options
If data is being replicated to HDFS then Circus Train will use DistCp to copy the data. The [DistCp documentation](https://hadoop.apache.org/docs/r2.7.2/hadoop-distcp/DistCp.html) provides detailed information on the options and their meanings but these are also summarised below.

          copier-options:
            file-attribute: replication, blocksize, user, group, permission, checksumtype, acl, xattr, times
            preserve-raw-xattrs: true
            atomic-commit: false
            atomic-work-path: /foo/bar/work
            blocking: true
            copy-strategy: uniformsize
            filters-file: /foo/bar/filters
            ignore-failures: false
            log-path: /foo/bar/log
            task-bandwidth: 100
            max-maps: 50
            skip-crc: false
            ssl-configuration-file: /foo/bar/ssl-config
            copier-factory-class: com.hotels.bdp.circustrain.distcpcopier.DistCpCopier

|Property|Required|Description|
|----|----|----|
|`copier-options.file-attribute`|No|Controls which file attributes should be preserved in the destination. See [org.apache.hadoop.tools.DistCpOptions.FileAttribute](https://github.com/apache/hadoop/blob/release-2.7.1/hadoop-tools/hadoop-distcp/src/main/java/org/apache/hadoop/tools/DistCpOptions.java#L74) for allowed values. For replications to HDFS this defaults to `replication`, `blocksize`, `user`, `group`, `permission`, `checksumtype`, `xattr`.|
|`copier-options.preserve-raw-xattrs`|No|Flat that indicates that raw X attributes should be preserved in the destination. For replications to HDFS file systems this defaults to to `true`.|
|`copier-options.atomic-commit`|No|Instructs DistCp whether to copy the source data to a temporary target location, and then move the temporary target to the final-location atomically. Defaults to `false`.|
|`copier-options.atomic-work-path`|No|Path to store temporary files used in the `atomic-commit` option.|
|`copier-options.copy-strategy`|No|Which strategy to use when copying the data, valid values are `dynamic`, `uniformsize`. By default, `uniformsize` is used (i.e. map tasks are balanced on the total size of files copied by each map. Similar to legacy.) If `dynamic` is specified, `DynamicInputFormat` is used instead.||
|`copier-options.ignore-failures`|No|This option will keep more accurate statistics about the copy than the default case. It also preserves logs from failed copies, which can be valuable for debugging. Finally, a failing map will not cause the job to fail before all splits are attempted. Defaults to `false`.|
|`copier-options.log-path`|No|Location of the log files generated by the job. Defaults to `null` which means log files will be written to `JobStagingDir/_logs`.|
|`copier-options.task-bandwidth`|No|Number of MiB/second that map tasks can consume. Map throttles back its bandwidth consumption during a copy, such that the net bandwidth used tends towards the specified value. Defaults to `100`.|
|`copier-options.max-maps`|No|Maximum number of map tasks used to copy files. Defaults to `50`.|
|`copier-options.skip-crc`|No|Controls whether CRC computation is skipped. Defaults to `false`.|
|`copier-options.ssl-configuration-file`|No|Path to the SSL configuration file to use for `hftps://`. Defaults to `null`.|
|`copier-options.ignore-missing-partition-folder-errors`|No|Boolean flag, if set to `true` will ignore errors from DistCp that normally fail the replication. DistCp normally fails when a partition is found in the metadata that is missing on HDFS (Default DistCp behavior). Defaults to `false` (so replication will fail).|
|`copier-options.copier-factory-class`|No|Controls which copier is used for replication if provided.|

##### S3MapReduceCp copier options
If data is being replicated from HDFS to S3 then Circus Train will use a customized, improved version of DistCp to copy the data. The options are summarised below.

          copier-options:
            credential-provider:
            task-bandwidth: 100
            storage-class: Standard
            s3-server-side-encryption: true
            region:
            multipart-upload-chunk-size: 5
            multipart-upload-threshold: 16
            max-maps: 20
            num-of-workers-per-map: 20
            copy-strategy: uniformsize
            ignore-failures: false
            log-path:
            copier-factory-class: com.hotels.bdp.circustrain.s3mapreducecpcopier.S3MapReduceCpCopier

| Property|Required|Description|
|----|----|----|
| `copier-options.credential-provider`|No|Path to the JCE key store with the AWS credentials. Defaults to the path specified in `security.credential-provider`. See [Replication configuration reference](#replication-configuration-reference) for details.|
| `copier-options.task-bandwidth`|No|Number of MB/second that Mappers can consume. A Mapper will throttle back its bandwidth consumption during a copy, such that the net bandwidth used tends towards the specified value. No limit by default.|
| `copier-options.storage-class`|No|S3 storage class. See IDs in [com.amazonaws.services.s3.model.StorageClass](http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/model/StorageClass.html#enum_constant_detail). Defaults to `null` which means default storage class, i.e. `STANDARD`.|
| `copier-options.s3-server-side-encryption`|No|Whether to enable server side encryption. Defaults to `true`.|
| `copier-options.region`|No|AWS Region for the S3 client. Defaults to `null` which means S3MapReduceCP will interrogate AWS for the target bucket location.|
| `copier-options.multipart-upload-chunk-size`|No|Size of multipart chunks in MB. Defaults to `5`.|
| `copier-options.multipart-upload-threshold`|No|Size threshold in MB for Amazon S3 object after which multi-part copy is initiated. Defaults to `16`.|
| `copier-options.max-maps`|No|Maximum number of map tasks used to copy files. Defaults to `20`.|
| `copier-options.num-of-workers-per-map`|No|Number of upload workers to use for each Mapper. Defaults to `20`.|
| `copier-options.copy-strategy`|No|Which strategy to use when copying the data, valid values are `dynamic`, `static` (A.K.A. `uniformsize`.) By default, `uniformsize` is used (i.e. map tasks are balanced on the total size of files copied by each map.) If `dynamic` is specified, `DynamicInputFormat` is used instead.|
| `copier-options.ignore-failures`|No|This option will keep more accurate statistics about the copy than the default case. It also preserves logs from failed copies, which can be valuable for debugging. Finally, a failing map will not cause the job to fail before all splits are attempted. Defaults to `false`.|
| `copier-options.log-path`|No|Location of the log files generated by the job. Defaults to `null` which means log files will be written to `JobStagingDir/_logs`.|
| `copier-options.s3-endpoint-uri`|No|URI of the S3 end-point used by the S3 client. Defaults to `null` which means the client will select the end-point.|
| `copier-options.upload-retry-count`|No|Maximum number of upload retries. Defaults to `3`|
| `copier-options.upload-retry-delay-ms`|No|Milliseconds between upload retries. The actual delay will be computed as `delay = attempt * copier-options.upload-retry-delay-ms` where `attempt` is the current retry number. Defaults to `300` ms.|
| `copier-options.upload-buffer-size`|No|Size of the buffer used to upload the stream of data. If the value is `0` the upload will use the value of the HDFS property `io.file.buffer.size` to configure the buffer. Defaults to `0`|
| `copier-options.canned-acl`|No|AWS Canned ACL name. See [Access Control List (ACL) Overview](https://docs.aws.amazon.com/AmazonS3/latest/dev/acl-overview.html#canned-acl) for possible values. If not specified `S3MapReduceCp` will not specify any canned ACL.|
| `copier-options.copier-factory-class`|No|Controls which copier is used for replication if provided.|

##### S3 to S3 copier options
If data is being replicated from S3 to S3 then Circus Train will use the AWS S3 API to copy data between S3 buckets. Using the AWS provided APIs no data needs to be downloaded or uploaded to the machine on which Circus Train is running but is copied by AWS internal infrastructure and stays in the AWS network boundaries. Assuming the correct bucket policies are in place cross region and cross account replication is supported. We are using the [TransferManager](https://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/transfer/TransferManager.html) to do the copying and we expose its options via copier-options see the table below. Given the source and target buckets Circus-Train will try to infer the region from them.

|Property|Required|Description|
|----|----|----|
|`copier-options.s3s3-multipart-copy-threshold-in-bytes`|No|Default value should be OK for most replications. See [TransferManagerConfiguration](https://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/transfer/TransferManagerConfiguration.html)|
|`copier-options.s3s3-multipart-copy-part-size-in-bytes`|No|Default value should be OK for most replications. See [TransferManagerConfiguration](https://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/transfer/TransferManagerConfiguration.html)|
|`copier-options.s3-endpoint-uri`|No|URI of the S3 end-point used by the S3 client. Defaults to `null` which means the client will select the end-point.|
|`copier-options.s3-server-side-encryption`|No|Whether to enable server side encryption. Defaults to `false`.|
|`copier-options.canned-acl`|No|AWS Canned ACL name. See [Access Control List (ACL) Overview](https://docs.aws.amazon.com/AmazonS3/latest/dev/acl-overview.html#canned-acl) for possible values. If not specified `S3S3Copier` will not specify any canned ACL.|
|`copier-options.copier-factory-class`|No|Controls which copier is used for replication if provided.|
|`copier-options.s3s3copier-retry-maxattempts`|No|Controls the maximum number of attempts if AWS throws an error during copy. Default value is 3.|

### S3 Secret Configuration
When configuring a job for replication to or from S3, the AWS access key and secret key with read/write access to the configured S3 buckets must be supplied. To protect these from being exposed in the job's Hadoop configuration, Circus Train expects them to be stored using the Hadoop Credential Provider and the JCEKS URL provided in the Circus Train configuration `security.credential-provider` property. This property is only required if a specific set of credentials is needed or if Circus Train runs on a non-AWS environment. If it is not set then the credentials of the instance where Circus Train runs will be used - note this scenario is only valid when Circus Train is executed on an AWS environment, i.e. EC2/EMR instance.

To add your existing AWS keys for a new replication job run the following commands as the user that will be executing Circus Train and pass in your keys when prompted:

        hadoop credential create access.key -provider <provider>
        hadoop credential create secret.key -provider <provider>

This needs to be done on a machine where the hadoop binary is present (e.g. a tenant node).

Note:

* For each command you will be prompted to enter the secret twice.
* We recommend that the `<provider>` be of the format `jceks://hdfs[@namenode:port]/foo/bar/aws-<account>.jceks`.
* To verify the credentials have been stored, run `hadoop credential list -provider <provider>`
* See the Hadoop documentation for further information about the [Hadoop Credential Provider](https://hadoop.apache.org/docs/r2.7.2/hadoop-project-dist/hadoop-common/CommandsManual.html#credential).

#### Configuring a SSH tunnel
Circus Train can be configured to use a SSH tunnel to access a remote Hive metastore in cases where certain network restrictions prevent a direct connection from the machine running Circus Train to the machine running the Thrift Hive metastore service. A SSH tunnel consists of one or more hops or jump-boxes. The connection between each pair of nodes requires a user - which if not specified defaults to the current user - and a private key to establish the SSH connection.

As outlined above the `metastore-tunnel` property is used to configure Circus train to use a tunnel. The tunnel `route` expression is described with the following <a href="https://en.wikipedia.org/wiki/Extended_Backus%E2%80%93Naur_Form">EBNF</a>:

    path = path part, {"->", path part} ;
    path part = {user, "@"}, hostname ;
    user = ? user name ? ;
    hostname = ? machine name with or without domain name ? ;

For example, if the Hive metastore runs on the host _hive-server-box_ which can only be reached first via _bastion-host_ and then _jump-box_ then the SSH tunnel route expression will be:

    bastion-host -> jump-box -> hive-server-box

If _bastion-host_ is only accessible by user _ec2-user_, _jump-box_ by user _user-a_ and _hive-server-box_ by user _hadoop_ then the expression above becomes:

    ec2-user@bastion-host -> user-a@jump-box -> hadoop@hive-server-box

Once the tunnel is established Circus Train will set up port forwarding from the local machine specified in `replica-catalog.metastore-tunnel.localhost` to the remote machine specified in `replica-catalog.hive-metastore-uris`. The last node in the tunnel expression doesn't need to be the Thrift server, the only requirement is that the this last node must be able to communicate with the Thrift service. Sometimes this is not possible due to firewall restrictions so in these cases they must be the same.

Note that all the machines in the tunnel expression must be included in the *known_hosts* file and the keys required to access each box must be set in `replica-catalog.metastore-tunnel.private-keys`. For example, if _bastion-host_ is authenticated with _bastion.pem_ and both _jump-box_ and _hive-server-box_ are authenticated with _emr.pem_ then the property must be set as`replica-catalog.metastore-tunnel.private-keys=<path-to-ssh-keys>/bastion.pem, <path-to-ssh-keys>/emr.pem`.

The following configuration snippets show a few examples of valid tunnel expressions. These fragments can be included under either `source-catalog` or `replica-catalog` depending on the use case.

##### Simple tunnel to metastore server
        hive-metastore-uris: thrift://metastore.domain:9083
        metastore-tunnel:
          route: user@metastore.domain
          private-keys: /home/user/.ssh/user-key-pair.pem
          known-hosts: /home/user/.ssh/known_hosts

##### Simple tunnel to cluster node with current user
        hive-metastore-uris: thrift://metastore.domain:9083
        metastore-tunnel:
          route: cluster-node.domain
          private-keys: /home/run-as-user/.ssh/key-pair.pem
          known-hosts: /home/run-as-user/.ssh/known_hosts

##### Bastion host to cluster node with different users and key-pairs
        hive-metastore-uris: thrift://metastore.domain:9083
        metastore-tunnel:
          route: bastionuser@bastion-host.domain -> user@cluster-node.domain
          private-keys: /home/run-as-user/.ssh/bastionuser-key-pair.pem, /home/run-as-user/.ssh/user-key-pair.pem
          known-hosts: /home/run-as-user/.ssh/known_hosts

##### Bastion host to cluster node with same user
        hive-metastore-uris: thrift://metastore.domain:9083
        metastore-tunnel:
          route: user@bastion-host.domain -> user@cluster-node.domain
          private-keys: /home/user/.ssh/user-key-pair.pem
          known-hosts: /home/user/.ssh/known_hosts

##### Bastion host to cluster node with current user
        hive-metastore-uris: thrift://metastore.domain:9083
        metastore-tunnel:
          route: bastion-host.domain -> cluster-node.domain
          private-keys: /home/run-as-user/.ssh/run-as-user-key-pair.pem
          known-hosts: /home/run-as-user/.ssh/known_hosts

##### Bastion host to metastore via jump-box with different users and key-pairs
        hive-metastore-uris: thrift://metastore.domain:9083
        metastore-tunnel:
          route: bastionuser@bastion-host.domain -> user@jump-box.domain -> hive@metastore.domain
          private-keys: /home/run-as-user/.ssh/bastionuser-key-pair.pem, /home/run-as-user/.ssh/user-key-pair.pem, /home/run-as-user/.ssh/hive-key-pair.pem
          known-hosts: /home/run-as-user/.ssh/known_hosts

### Instance Globals
Circus Train declares some global configuration properties with sensible default values that can be overridden. These mainly control where Circus Train stores the data files it needs in order to operate and are described in more detail below.

|Property|Required|Description|
|----|----|----|
|`instance.home`|No|The path to the home directory of the Circus Train instance. Typically this would be the folder containing the conf and housekeeping data directories. Defaults to the user's home directory (`${user.home}`.|
|`instance.name`|No|A name to be used for constructing the default housekeeping H2 database location. Defaults to `${source-catalog.name}_${replica-catalog.name}`.|

### Profiles
Circus Train is able to use Spring Boot Profiles to further control configuration by allowing the activation of different sections of configuration for different operating conditions (e.g. environments).

#### Profiles within files
A YAML file is made up of multiple *documents*, separated by three hyphens (`---`). It is therefore possible to put the configuration for multiple environments in a single `circus-train.yml` file and use profiles within each document to conditionally activate these configuration elements. The example below shows how you could use profiles to control the Hive metastore URI based on whether a `dev`, `qa` or `prod` profile is active:

        source-catalog:
          name: on-premises-cluster
        replica-catalog:
          name: aws-data-warehouse
        table-replications:
          -
            ...
        ---
        spring:
          profiles: dev
        replica-catalog:
          hive-metastore-uris: thrift://<dev-host>.com:9083
        ---
        spring:
          profiles: qa
        replica-catalog:
          hive-metastore-uris: thrift://<qa-host>.com:9083
        ---
        spring:
          profiles: prod
        replica-catalog:
          hive-metastore-uris: thrift://<prod-host>.com:9083

### Configuring housekeeping
Housekeeping is the process that removes expired and orphaned data on the replica. Below is a YAML configuration fragment and description of the available configuration properties. Implementation of Housekeeping can be found [here](https://github.com/HotelsDotCom/housekeeping)

        housekeeping:
          expired-path-duration: P3D
          data-source:
            driver-class-name: org.h2.Driver 
            url: jdbc:h2:${housekeeping.h2.database};AUTO_SERVER=TRUE;DB_CLOSE_ON_EXIT=FALSE
            username: user
            password: secret

|Property|Required|Description|
|----|----|----|
|`housekeeping.h2.database`|No|The location of the h2 housekeeping database. Defaults to `${instance.home}/data/${instance.name}/housekeeping`. Refer to the [Instance Globals](#instance-globals) section for more details.|
|`housekeeping.schema-name`|No|Database schema name to use. Circus Train default: 'circus_train'|

For more details on Housekeeping configuration, including examples on how to override the H2 default database with something a bit more robust, please consult the [Housekeeping documentation](https://github.com/HotelsDotCom/housekeeping).


## Metric Reporting
Circus Train can be configured to output metrics to "standard out" as well as to Graphite. Most metrics generated by Circus Train are reported after a job has run and represent totals or averages for the duration of the entire job. However, the number of bytes replicated is reported while the copy process is running to allow a more granular view of bandwidth over time instead of just one value at the end. The frequency of reporting the metric can be controlled via the following properties (which show the default values, sending metrics every minute):

    metrics-reporter:
        period: 1
        time-unit: MINUTES

|Property|Required|Description|
|----|----|----|
|`metrics-reporter.period`|No|Period in between measurements, defaults to 1.|
|`metrics-reporter.time-unit`|No|TimeUnit for period e.g. MILLISECONDS, SECONDS, MINUTES, etc (see the enum java.util.concurrent.TimeUnit), defaults to MINUTES (needs to be upper-case to match the enum)|

### Graphite
Circus Train can be configured to send various metrics about a replication job to Graphite. Metrics are sent in the format:

        <prefix>.<namespace>[.database.table].<metric_name>

Note that `database` and `table` are only present for table level metrics and are automatically filled in by Circus Train. The following metrics are sent for each table/job.

|Metric Name|Description|
|----|----|
|`completion_code`|See [Exit codes](#exit-codes) for possible values.|
|`[database].[table].completion_code`|`1` for success or `-1` for failure.|
|`[database].[table].replication_time`|Milliseconds taken to complete the replication. This includes all metastore interactions as well as the replication process itself.|
|`[database].[table].bytes_replicated`|Number of bytes transmitted to the replica table location.|
|`[database].[table].[hadoop_counters]`|All other metrics (typically Hadoop Counters from DistCp and S3MepReduceCp jobs).|

The configuration options for Graphite are: 

        graphite:
          config: hdfs:///path/to/your/conf/cluster.properties
          host:
          prefix:
          namespace: com.company.<team>.circus-train.<application>

|Property|Required|Default/Example|Description|
|----|----|----|----|
|`graphite.config`|No|e.g. hdfs:///foo/bar/cluster.properties|Specifies where to pick up the default Graphite properties from. By default it picks up the `host` and `prefix` from the cluster.properties file on HDFS.|
|`graphite.host`|No|`hostname`:`port`|The hostname and port of the Graphite server. By default this is picked up from the `config` if provided.|
|`graphite.prefix`|No|e.g. `dev`|All metrics are prefixed with `prefix.namespace`. By default this is picked up from the `config` if provided.|
| `graphite.namespace`|Yes|e.g. `com.company.team.circus-train.application`|All metrics are prefixed with `prefix.namespace`. Formatting of this is important for reporting from Graphite.|

## Important Notes
* By default, the source Hadoop and Hive configurations are loaded from the environment.
* The replica database you are replicating into must already exist in the remote Hive metastore (i.e. Circus Train won't create this for you).
* You will need to have the relevant permissions to create snapshots on the source if you have this feature enabled.
* Removal of partitions is not replicated - i.e. if you remove a partition on the source it will remain on the replica.
* If replicating to/from AWS ensure that the EMR master node is in your 'known_hosts' file.

### Notes on Hadoop S3 FileSystems
The Hadoop S3 `FileSystem` landscape is chaotic and confusing. What truths apply in a vanilla Apache Hadoop cluster do not necessarily hold in an EMR cluster. Protocols map to different implementations in different environments, and implementations can lack necessary features. For Circus Train we are interested in the following properties:

* Optimal performance in EMR environments.
* MD5 data verification for transmissions.
* High-throughput transmission.
* Support for very large files.
* Copier execution taking place on-premises.
* Adherence to Amazon EMR recommendations.

We have spent time investigating the S3 `FileSystem` implementations provided by both the Apache Hadoop project and the Amazon EMR project. Additionally we've analysed the operation of multiple versions of both `DistCp` and `S3MapReduceCp`. With this in mind we believe that the best way to move data using Circus Train from an on-premises cluster to S3 for consumption by an EMR cluster is using `S3MapReduceCp`.

We have verified its operation end-to-end, from a file based in HortonWorks HDFS to a query executing in EMR Hive. As a user all you need do is specify your S3 paths using **only** the `s3://` protocol, Circus Train will handle the details. 

## Project layout
Below is a high level summary of the various modules that comprise Circus Train as they are laid out in version control.

* **circus-train-api:** Connector contracts to allow alternative integrations.
* **circus-train-avro:** Circus Train transformations to replicate Avro tables. [readme](circus-train-avro/README.md)
* **circus-train-aws:** AWS specific connector implementations. Scripts for setting up a test AWS/EMR replica.
* **circus-train-aws-sns:** Publishes replication events to Amazon SNS for workflow orchestration. [readme](circus-train-aws-sns/README.md)
* **circus-train-comparator:** Module used internally to compute a "Hive Diff" for comparison of source and replica data and metadata.
* **circus-train-core:** Main class, replication strategies.
* **circus-train-distcp-copier:** Builds a [DistCp](https://hadoop.apache.org/docs/r2.7.2/hadoop-distcp/DistCp.html) file copier.
* **circus-train-gcp:** Builds Google Storage file copier. [readme](circus-train-gcp/README.md)
* **circus-train-hive:** Common Hive-related helper classes.
* **circus-train-hive-view:** Components used to replicate views.
* **circus-train-housekeeping:** A database-backed module that stores orphaned replica paths in a table for later clean up. This module is loaded by activating the `housekeeping` [Spring Boot Profile](https://docs.spring.io/spring-boot/docs/current/reference/html/boot-features-profiles.html)
* **circus-train-integration-tests:** Circus Train integration tests.
* **circus-train-metrics:** Code used to enable reporting of Circus Train metrics to Graphite.
* **circus-train-package:** Package Circus Train a tgz.
* **circus-train-s3-mapreduce-cp:** Hadoop job based on [DistCp](https://hadoop.apache.org/docs/r2.7.2/hadoop-distcp/DistCp.html) optimised for AWS S3 data transfers. [readme](circus-train-s3-mapreduce-cp/README.md)
* **circus-train-s3-mapreduce-cp-copier:** Builds a `S3MapReduceCp` file copier using the package built in the `circus-train-s3-mapreduce-cp` module.
* **circus-train-s3-s3-copier:** Delegates file copy from on S3 location to another S3 location to the AWS infrastructure.
* **circus-train-tool-parent:** Sub-module for debug and maintenance tools. [readme](circus-train-tool-parent/README.md)
  * **circus-train-comparison-tool:** Tool that performs data and metadata comparisons, useful for debugging the "Hive Diff" feature.
  * **circus-train-filter-tool:** Tool that checks partition filter expressions in table replication configurations.
  * **circus-train-tool-core:** Code common across tool implementations.
  * **circus-train-tool:** Packages tools tgz.

## Extension points
Circus Train can be extended in various ways. This is an advanced feature that typically involves an implementation of an interface or an extension of existing classes. These are built separately from Circus Train and then placed on the classpath when Circus Train is run. Below is a summary of some common extension points.

* `ReplicationEventListeners` can send replication start, success, and failure notifications to other systems. They are easy to develop and are dynamically loaded.
* `Source` represents the cluster from which we are loading data. Designed to be reused for multiple tables/replications.
* `Replica` represents the cluster to which we are copying data. Designed to be reused for multiple tables/replications.
* `Replications` provide strategies for copying high level entities such as tables and partitions.
* `CopierFactories` provide means of abstracting the complexity of creating `Copiers`.
* `Copiers` move table data between cluster file systems.
* `LocationManagers` look after the source and replica locations and are an ideal point to implement both snapshot isolation and retired data clean-up.
* `CompositeCopierFactory` allows the provision of multiple copiers for the same table in order to add functionality to the copy process, e.g. introduce compaction, copy to multiple destinations, etc.

### Loading Extensions
Circus Train loads extensions using Spring's standard [ComponentScan](https://docs.spring.io/spring-framework/docs/current/javadoc-api/org/springframework/context/annotation/ComponentScan.html) mechanism. Users can add their own packages to be scanned by declaring `extension-packages` as a comma separated list of package names to their YAML.

Example:

        extension-packages: some.extension,another.extension

In order not to clash with Circus Train's internal components we recommended you use your own package structure for these extensions (i.e. do not use `com.hotels.circustrain` or any sub-packages of this). The classes involved in implementing the extensions need to be made available on Circus Train's CLASSPATH. If your extension implementations require any existing Circus Train Beans then `CircusTrainContext` can be `@Autowired` in.

### Metadata transformations
The following transformation interfaces can be implemented to manipulate metadata during replication. Note that transformations are loaded by Spring - so they must be annotated with `@Component` - and only one transformation of each type is allowed on the classpath. In case multiple transformations are required you can compose them into a single transformation function.

* `TableTransformation` is applied to the table metadata before the replica database is updated.
* `PartitionTransformation` is applied to the partition metadata before the replica database is updated.
* `ColumnStatisticsTransformation` is applied to the column statistics metadata of both table and partitions before the replica database is updated. The property `statsDesc.tblLevel` of `ColumnStatistics` can be used to determine if the statistics are for the table or a partition.

Relevant configuration: `table-replications[n].transform-options`
Example:

        table-replications:
          - ...
            transform-options:
               custom-transform-config-key1: someValue

### Developing a `ReplicationEventListener`
A high level summary that assumes some knowledge of the Spring Framework and Spring Boot. See `circus-train-aws-sns` for a concrete example.

* Create a new Maven project.
* Add `com.hotels.bdp:circus-train-api` as a dependency.
* Create a Spring context class to instantiate your listener's dependencies (clients etc.), mark it with `@Configuration` and mark factory methods with `@Bean`.
* Create a JavaBean that represents your listener's configuration, mark it with `@Configuration`. Optionally assign it a prefix with `@ConfigurationProperties`.
* Create your listener, implementing the  `com.hotels.bdp.circustrain.api.event.*Listener` interfaces as needed, mark it as a `@Component` and `@Autowire` its dependencies.
* Try to prevent blocking callers to your listener by invoking other systems asynchronously.
* In a script that invokes Circus Train, append an entries to the `CIRCUS_TRAIN_CLASSPATH` environment variable to add your listener jar file and its dependencies to the Circus Train classpath. 
* Add the package where your `Listener` is located to the `extension-packages` section of your replication YAML.

### Providing a custom `MetaStoreClientFactory`
`MetaStoreClientFactory` is responsible for creating the instances of the source and replica `CloseableMetaStoreClient`.
Simply mark your implementation with `@Component` or have a `@Configuration` create a `@Bean` and the package names
containing these classes to the `extension-packages` section of your YAML configuration.

The `accept` method must analyse the given metastore URL to determine whether the class can create a client for the metastore protocol. Circus Train will choose the first implementation in the classpath that accepts the metastore URL - the order in which these classes are loaded and interrogated is not controlled by Circus Train, i.e. the interrogation order in unpredictable and depends on the class loader.

Circus Train provides a `MetaStoreClientFactory` implementation out of the box that creates clients for the `thrift` protocol. 

### Implementing your own data copiers
New `CopierFactories` can be added just by extending the `CopierFactory` interface. Circus Train relies on Spring to detect a `CopierFactory` so each instance of such interface can either be instantiated in a Spring `@Configuration` class or the class itself can be annotated with `@Component` and `@Order`. The `@Order` annotation is used to establish precedence with respect to other `CopierFactories` that support the same schema.

The copier factories `DistCpCopierFactory`, `S3MapReduceCpCopierFactory` and  `S3S3CopierFactory`, which are provided out of the box, take the lowest precedence allowing users to override them with their own implementations.

### Implementing data transformations in the copying phase
The copy functionality of existing `Copiers` can be extended by declaring an instance of `CompositeCopierFactory` which allows a set of `Copiers` to be executed for one replication. This opens the possibility of adding functionality to the replication, like performing compaction before or after moving the data to its target location.

`CompositeCopierFactory` will support the same schema supported by the first `CopierFactory` in the delegates list.

All `Copiers` in the delegates list share the same set of configuration properties specified in `copier-options`. This set of properties can be used to control the behaviour of specific functionalities of each `Copier`. Users can add custom properties in this configuration section as well as set the values of any out-of-the-box `Copier` - refer to the [Copier options](#copier-options) section for details.

## Connecting to a housekeeping DB
By default Circus Train uses an [H2](http://www.h2database.com) file-based database to store information about dereferenced partition locations in the replica catalog. This should be transparent to most end users.

## Configuration Examples
Below are a number of configuration fragments which can be used as a starting point for creating your own configuration files. These are for reference and will obviously need host names and various other values changed to suit your setup. Also keep in mind that database and table locations in Hive are fully qualified paths therefore if using AWS then EMR must be able to translate DNS names. If a DNS service is not available then each EMR node - master and slaves - must include an entry in _etc/hosts_ for each domain name in the Circus Train configuration file.

### Circus Train running on on-premises cluster A synchronizing tables from on-premises cluster A to on-premises cluster B
This configuration below shows how you could run Circus Train to replicate data between two on-premises clusters - for example you may want to replicate data between the on-premises `DEV` cluster (referred to as "cluster A") and the on-premises `QA` cluster (referred to as "cluster B"). Circus Train runs on `DEV` and it will push data to `QA`.

    source-catalog:
      name: dev
    replica-catalog:
      name: qa
      hive-metastore-uris: thrift://hiveQA.onpremises.domain:9083
    table-replications:
      - source-table:
          database-name: dev_db
          table-name: dev_table
        replica-table:
          database-name: qa_db
          table-name: qa_table
          table-location: hdfs://nameNodeQA.onpremises.domain:8020/<path-to-qa_db>/qa_table

### Circus Train running on on-premises cluster A synchronizing tables from on-premises cluster B to on-premises cluster A
This configuration below shows how you could run Circus Train to replicate data between two on-premises clusters - for example you may want to replicate data between the on-premises `DEV` cluster (referred to as "cluster A") and the on-premises `QA` cluster (referred to as "cluster B"). Circus Train runs on `DEV` and it will pull data from `QA`.

    source-catalog:
      name: qa
      hive-metastore-uris: thrift://hiveQA.onpremises.domain:9083
    replica-catalog:
      name: dev
      hive-metastore-uris: thrift://hiveDEV.onpremises.domain:9083
    table-replications:
      - source-table:
          database-name: qa_db
          table-name: qa_table
        replica-table:
          database-name: dev_db
          table-name: dev_table
          table-location: hdfs://nameNodeDEV.onpremises.domain/<path-to-dev_db>/dev_table

### Circus Train running on on-premises cluster A synchronizing tables from on-premises cluster A to EMR cluster B
This configuration below shows how you could run Circus Train to replicate data between an on-premises cluster and EMR - for example you may want to replicate data between the on-premises `DEV` cluster (referred to as "cluster A") and the EMR `AWS` cluster (referred to as "cluster B"). Circus Train runs on `DEV` and it will push data to `AWS`.

    source-catalog:
      name: dev
    replica-catalog:
      name: aws
      hive-metastore-uris: thrift://hiveAWS.amazonaws.com:9083
    security:
      credential-provider: jceks://hdfs/<path-to-aws-credentials>/circus-train-credentials.jceks
    table-replications:
      - source-table:
          database-name: dev_db
          table-name: dev_table
        replica-table:
          database-name: aws_db
          table-name: aws_table
          table-location: s3://<data-bucket>/<path-to-aws_db>/aws_table

### Circus Train running on on-premises cluster A synchronizing tables from EMR cluster B to on-premises cluster A
This configuration below shows how you could run Circus Train to replicate data between an on-premises cluster and EMR - for example you may want to replicate data between the on-premises `DEV` cluster (referred to as "cluster A") and the EMR `AWS` cluster (referred to as "cluster B"). Circus Train runs on `DEV` and it will pull data from `AWS`.

    source-catalog:
      name: aws
      hive-metastore-uris: thrift://hiveAWS.amazonaws.com:9083
    replica-catalog:
      name: dev
      hive-metastore-uris: thrift://hiveDEV.onpremises.domain:9083
    security:
      credential-provider: jceks://hdfs//<path-to-aws-credentials>/circus-train-credentials.jceks
    table-replications:
      - source-table:
          database-name: aws_db
          table-name: aws_table
        replica-table:
          database-name: dev_db
          table-name: dev_table
          table-location: hdfs://nameNodeDEV.onpremises.domain/<path-to-dev_db>/dev_table

### Circus Train running on EMR cluster A synchronizing tables from EMR cluster A to on-premises cluster B
This configuration below shows how you could run Circus Train to replicate data between EMR and an on-premises cluster - for example you may want to replicate data between the `AWS` cluster (referred to as "cluster A") and the on-premises `DEV` cluster (referred to as "cluster B"). Circus Train runs on `AWS` and it will push data to `DEV`.

Note that usually the EMR user who runs Circus Train is the default EMR user _hadoop_ so prior to running Circus Train you may need to export the user who owns the table on the on-premises clusters, e.g. `export HADOOP_USER_NAME=hwwetl`.

    source-catalog:
      name: aws
    replica-catalog:
      name: dev
      hive-metastore-uris: thrift://hiveDEV.onpremises.domain:9083
    copier-options:
      tmp-dir: hdfs:///tmp/circus-train/
    table-replications:
      - source-table:
          database-name: aws_db
          table-name: aws_table
        replica-table:
          database-name: dev_db
          table-name: dev_table
          table-location: hdfs://nameNodeDEV.onpremises.domain/<path-to-dev_db>/dev_table

### Circus Train running on EMR cluster A synchronizing tables from on-premises cluster B to EMR cluster A
This configuration below shows how you could run Circus Train to replicate data between EMR and an on-premises cluster - for example you may want to replicate data between the `AWS` cluster (referred to as "cluster A") and the on-premises `DEV` cluster (referred to as "cluster B"). Circus Train runs on `AWS` and it will pull data from `DEV`.
    
    source-catalog:
      name: dev
      hive-metastore-uris: thrift://hiveDEV.onpremises.domain:9083
    replica-catalog:
      name: aws
      hive-metastore-uris: thrift://hiveAWS.amazonaws.com:9083
    copier-options:
      tmp-dir: hdfs:///tmp/circus-train/
    table-replications:
      - source-table:
          database-name: dev_db
          table-name: dev_table
        replica-table:
          database-name: aws_db
          table-name: aws_table
          table-location: s3://<data-bucket>/<path-to-aws_db>/aws_table

### Circus Train running on EMR cluster A synchronizing tables from EMR cluster A to EMR cluster B
This configuration below shows how you could run Circus Train to replicate data between two EMR clusters - for example you may want to replicate data between EMR `AWS-A` cluster (referred to as "cluster A") and the EMR `AWS-B` cluster (referred to as "cluster B"). Circus Train runs on `AWS-A` and it will push data to `AWS-B`.

Note that the Circus Train configuration only supports one set of AWS credentials so the AWS user must have access to both the source and destination buckets. This can be achieved via IAM roles and policies.

    source-catalog:
      name: aws-a
      hive-metastore-uris: thrift://hiveAWS-A.amazonaws.com:9083
    replica-catalog:
      name: aws-b
      hive-metastore-uris: thrift://hiveAWS-B.amazonaws.com:9083
    copier-options:
      tmp-dir: hdfs:///tmp/circus-train/
    table-replications:
      - source-table:
          database-name: aws-a_db
          table-name: aws-a_table
        replica-table:
          database-name: aws-b_db
          table-name: aws-b_table
          table-location: s3://<aws-b-data-bucket>/<pata-to-aws-b_db>/aws-b_table

### Circus Train running on EMR cluster A synchronizing tables from EMR cluster B to EMR cluster A
This configuration below shows how you could run Circus Train to replicate data between two EMR clusters - for example you may want to replicate data between EMR `AWS-A` cluster (referred to as "cluster A") and the EMR `AWS-B` cluster (referred to as "cluster B"). Circus Train runs on `AWS-A` and it will pull data from `AWS-B`.

Note that the Circus Train configuration only supports one set of AWS credentials so the AWS user must have access to both the source and destination buckets. This can be achieved via IAM roles and policies.

    source-catalog:
      name: aws-b
      hive-metastore-uris: thrift://hiveAWS-B.amazonaws.com:9083
    replica-catalog:
      name: aws-a
      hive-metastore-uris: thrift://hiveAWS-A.amazonaws.com:9083
    copier-options:
      tmp-dir: hdfs:///tmp/circus-train/
    security:
      credential-provider: jceks://hdfs/<path-to-aws-credentials-on-hdfs-A>/circus-train-credentials.jceks
    table-replications:
      - source-table:
          database-name: aws-b_db
          table-name: aws-b_table
        replica-table:
          database-name: aws-a_db
          table-name: aws-a_table
          table-location: s3://<aws-a-data-bucket>/<pata-to-aws-a_db>/aws-a_table

# Contact

## Mailing List
If you would like to ask any questions about or discuss Circus Train please join our mailing list at 

  [https://groups.google.com/forum/#!forum/circus-train-user](https://groups.google.com/forum/#!forum/circus-train-user)
  
# Credits
Created by [Elliot West](https://github.com/teabot), [Daniel del Castillo](https://github.com/ddcprg), [Patrick Duin](https://github.com/patduin), [Dave Maughan](https://github.com/nahguam) & [Courtney Edwards](https://github.com/courtsvii) with thanks to: [Adrian Woodhead](https://github.com/massdosage), [Dave Bauman](https://github.com/baumandm), Jose Nuñez Izu and Oscar Mateos Ventura.

The Circus Train logo uses the [Ewert font](http://www.1001fonts.com/ewert-font.html) by [Johan Kallas](http://www.1001fonts.com/users/kallasjohan/) under the [SIL Open Font License (OFL)](http://scripts.sil.org/cms/scripts/page.php?site_id=nrsi&id=OFL).

# Legal
This project is available under the [Apache 2.0 License](http://www.apache.org/licenses/LICENSE-2.0.html).

Copyright 2016-2019 Expedia Inc.
