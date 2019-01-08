## TBD
### Changed
* Refactored project to remove checkstyle and findbugs warnings, which does not impact functionality.
* Upgraded `hotels-oss-parent` to 2.3.5 (was 2.3.3).
### Added
* Added replication-strategy configuration that can be used to support propagating deletes (drop table/partition operations). See [README.md](https://github.com/HotelsDotCom/circus-train#replication-strategy) for more details.

## 13.1.0 - 2018-12-20
### Changed
* Housekeeping can be configured to control query batch size, this controls memory usage. See [#40](https://github.com/HotelsDotCom/housekeeping/issues/40).
* Housekeeping readme moved to Housekeeping project. See [#31](https://github.com/HotelsDotCom/housekeeping/issues/31).
* Upgraded Housekeeping library to also store replica database and table name in Housekeeping database. See [#30](https://github.com/HotelsDotCom/housekeeping/issues/30).
* Upgraded `hotels-oss-parent` pom to 2.3.3 (was 2.0.6). See [#97](https://github.com/HotelsDotCom/circus-train/issues/97).

## 13.0.0 - 2018-10-15
### Changed
* Narrowed component scanning to be internal base packages instead of `com.hotels.bdp.circustrain`. See [#95](https://github.com/HotelsDotCom/circus-train/issues/95). Note this change is _not_ backwards compatible for any Circus Train extensions that are in the `com.hotels.bdp.circustrain` package - these were in effect being 
implicitly scanned and loaded but won't be now. Instead these extensions will now need to be added using Circus Train's [standard extension loading mechanism](https://github.com/HotelsDotCom/circus-train#loading-extensions).
* Upgraded `jackson.version` to 2.9.7 (was 2.6.6), `aws-jdk.version` to 1.11.431 (was 1.11.126) and `httpcomponents.httpclient.version` to 4.5.5 (was 4.5.2). See [#91](https://github.com/HotelsDotCom/circus-train/issues/91).
* Refactored general metastore tunnelling code to leverage hcommon-hive-metastore libraries. See [#85](https://github.com/HotelsDotCom/circus-train/issues/85).
* Refactored the remaining code in `core.metastore` from `circus-train-core` to leverage hcommon-hive-metastore libraries.

## 12.1.0 - 2018-08-08
### Changed
* circus-train-gcp: avoid temporary copy of key file to `user.dir` when using absolute path to Google Cloud credentials file by transforming it into relative path.
* circus-train-gcp: relative path can now be provided in the configuration for the Google Cloud credentials file.

## 12.0.0 - 2018-07-13
### Changed
* circus-train-vacuum-tool moved into [Housekeeping](https://github.com/HotelsDotCom/housekeeping) project under the module housekeeping-vacuum-tool.
* Configuration classes moved from Core to API sub-project. See [#78](https://github.com/HotelsDotCom/circus-train/issues/782).

## 11.5.2 - 2018-06-15
### Changed
* Refactored general purpose Hive metastore code to leverage hcommon-hive-metastore libraries. See [#72](https://github.com/HotelsDotCom/circus-train/issues/72).
### Fixed
* Avro schemas were not being replicated when a avro.schema.url without a scheme was specified. See [#74](https://github.com/HotelsDotCom/circus-train/issues/74)

# 11.5.1 - 2018-05-24
### Fixed
* Avro schemas were not being replicated when a HA NameNode is configured and the Avro replication feature is used. See [#69](https://github.com/HotelsDotCom/circus-train/issues/69).

# 11.5.0 - 2018-05-24
### Added
* Add SSH timeout and SSH strict host key checking capabilities. [#64](https://github.com/HotelsDotCom/circus-train/issues/64).
### Changed
* Using hcommon-ssh-1.0.1 dependency to fix issue where metastore exceptions were lost and not propagated properly over tunnelled connections.
* Replace SSH support with [hcommon-ssh](https://github.com/HotelsDotCom/hcommon-ssh) library. [#46](https://github.com/HotelsDotCom/circus-train/issues/46).
### Fixed
* Housekeeping was failing when attempting to delete a path which no longer exists on the replica filesystem. Upgraded Circus Train's Housekeeping dependency to a version which fixes this bug. See [#61](https://github.com/HotelsDotCom/circus-train/issues/61).

# 11.4.0 - 2018-04-11
### Added
* Ability to select Copier via configuration. See [#55](https://github.com/HotelsDotCom/circus-train/issues/55).

# 11.3.1 - 2018-03-21
### Changed
* Clearer replica-check exception message. See [#47](https://github.com/HotelsDotCom/circus-train/issues/47).
### Fixed
* S3-S3 Hive Diff calculating incorrect checksum on folders. See [#49](https://github.com/HotelsDotCom/circus-train/issues/49).

# 11.3.0 - 2018-02-27
### Changed
* SNS message now indicates if message was truncated. See [#41](https://github.com/HotelsDotCom/circus-train/issues/41).
* Exclude Guava 17.0 in favour of Guava 20.0 for Google Cloud library compatibility.
* Add dependency management bom for Google Cloud dependencies.
### Fixed
* Backwards compatibility with Hive 1.2.x.

# 11.2.0 - 2018-02-16
### Added
* Added ability to configure AWS Server Side Encryption for `S3S3Copier` via `copier-options.s3-server-side-encryption` configuration property.
### Changed
* Upgrade housekeeping to version 1.0.2.

# 11.1.1 - 2018-02-15
### Fixed
* Google FileSystem classes not being placed onto the mapreduce.application.classpath in S3MapReduceCp and DistCp mapreduce jobs.
### Changed
* Google FileSystem and S3 FileSystems added to mapreduce.application.classpath in circus-train-gcp and circus-train-aws respectively.

# 11.1.0 - 2018-02-05
### Fixed
* https://github.com/HotelsDotCom/circus-train/issues/23 - Housekeeping failing due to missing credentials.
### Added
* Added `replicaTableLocation`, `replicaMetastoreUris` and  `partitionKeys` to SNS message.
### Changed
* SNS Message `protocolVersion` changed from "1.0" to "1.1".
* Updated documentation for circus-train-aws-sns module (full reference of SNS message format, more examples).
* Fixed references to README.md in command line runner help messages to point to correct GitHub locations.

# 11.0.0 - 2018-01-16
### Changed
* Upgraded Hive version from 1.2.1 to 2.3.2 (changes are backwards compatible).
* Upgraded Spring Platform version from 2.0.3.RELEASE to 2.0.8.RELEASE.
* Replaced `TunnellingMetaStoreClient` "concrete" implementation with a Java reflection `TunnellingMetaStoreClientInvocationHandler`. 
* Replicating a partitioned table containing no partitions will now succeed instead of silently not replicating the table metadata.
* Most functionality from Housekeeping module moved to https://github.com/HotelsDotCom/housekeeping.

# 10.0.0 - 2017-11-21
### Changed
* Maven group ID changed to _com.hotels_.
* Exclude logback in parent POM.
* First open source release.
* Various small code cleanups.

# 9.2.0 - 2017-11-20
### Fixed
* `S3S3Copier` captures cross region replications from US-Standard AWS regions.
### Added
* Mock S3 end-point for HDFS-S3 and S3-S3 replications.
* New `S3MapreduceCp` properties to control the size of the buffer used by the S3 `TransferManager` and to control the upload retries of the S3 client. Refer to _[README.md](README.md)_ for details.

# 9.1.1 - 2017-10-17
### Fixed
* `EventIdExtractor` RegEx changed so that it captures new event ID's and legacy event ID's.
* Add read limit to prevent AWS library from trying to read more data than the size of the buffer provided by the Hadoop `FileSystem`.


# 9.1.0 - 2017-10-03
### Added 
* _circus-train-housekeeping_ support for storing housekeeping data in JDBC compliant SQL databases.
### Changed
* _circus-train-parent_ updated to inherit from _hww-parent_ version 12.1.3.

# 9.0.2 - 2017-09-20
### Added
* Support for replication of Hive views.
### Changed
* Removed _circus-train-aws_ dependency on internal patched _hadoop-aws_.

# 9.0.1 - 2017-09-14
### Fixed 
* Fixed error when replicating partitioned tables with empty partitions.

# 9.0.0 - 2017-09-05
### Changed
* Removed _circus-train-aws_ dependency from circus-train-core.
* Circus Train Tools to be packaged as TGZ.
* Updated to parent POM 12.1.0 (latest versions of dependencies and plugins).
* Relocate only Guava rather than Google Cloud Platform Dependencies + Guava
### Removed
* `CircusTrainContext` interface as it was a legacy leftover way to make CT pluggable.
### Fixed
* Fixed broken Circus Train Tool Scripts.

# 8.0.0 - 2017-07-28
### Added
* S3/HDFS to GS Hive replication.
* Support for users to be able to specify a list of `extension-packages` in their YAML configuration. This adds the specified packages to Spring's component scan, thereby allowing the loading of extensions via standard Spring annotations, such as `@Component` and `@Configuration`.
* Added _circus-train-package_ module that builds a TGZ file with a runnable _circus-train_ script.
### Changed
* Changed public interface `com.hotels.bdp.circustrain.api.event.TableReplicationListener`, for easier error handling.
### Removed
* RPM module has been pulled out to a top-level project.

# 7.0.0
### Added
* New and improved HDFS-to-S3 copier.
### Changed
* Changed default `instance.home` from `$user.dir` to `$user.home`. If you relied on the `$user.dir` please set the `instance.home` variable in the YAML config.
### Fixed
* Fixed issue in LoggingListener where the wrong number of altered partitions was reported.

# 6.0.0
* Fixed issue with the _circus-train-aws-sns_ module.

# 5.2.0
* Added S3 to S3 replication.
* Should have been major release. The public `com.hotels.bdp.circustrain.api.copier.CopierFactory.supportsSchemes` method has changed signature. Please adjust your code if you rely on this for Circus Train extensions. 

# 5.1.1
* Clean up of AWS Credential Provider classes.
* Fixed tables in documentation to display correctly.

# 5.1.0
* Added support for skipping missing partition folder errors.
* Added support for transporting AvroSerDe files when the avro.schema.url is specified within the _SERDEPROPERTIES_ rather than the _TBLPROPERTIES_.
* Fixed bug where table-location matching avro schema base-url causes an IOException to be thrown in future reads of the replica table.
* Added transformation config per table replication (Used in Avro SerDe transformation)
* Replaced usage of _reflections.org_ with Spring's scanning provider.

# 5.0.2
* Documented methods for implementing custom copiers and data transformations.
* Cleaned up copier options configuration classes.

# 5.0.1
* Support for composite copiers.

# 4.0.0
* Extensions for `HiveMetaStoreClientFactory` to allow integration with AWS Athena.
* Updated to extend _hdw-parent_ 9.2.2 which in turn upgrades _hive.version_ to 1.2.1000.2.4.3.3-2.

# 3.4.0
* Support for downloading Avro schemas from the URI on the source table and uploading it to a user specified URL on replication.
* Multiple transformations can now be loaded onto the classpath for application on replication rather than just one.

# 3.3.1
* Update to new parent with HDP dependency updates for the Hadoop upgrade.
* Fixed bug where incorrect table name resulted in a NullPointerException.

# 3.3.0
* Added new "replication mode: METADATA_UPDATE" feature which provides the ability to replicate metadata only which is useful for updating the structure and configuration of previously replicated tables.

# 3.2.0
* Added new "replication mode: METADATA_MIRROR" feature which provides the ability to replicate metadata only, pointing the replica table at the original source data locations.

# 3.1.1
* Replication and Housekeeping can now be executed in separate processes.
  * Add the option `--modules=replication` to the scripts `circus-train.sh` to perform replication only.
  * Use the scripts `housekeeping.sh` and `housekeeping-rush.sh` to perform housekeeping in its own process.

# 3.0.2
* Made scripts and code workable with HDP-2.4.3.

# 3.0.1
* Fixed issue where wrong replication was listed as the failed replication.

# 3.0.0
* Support for replication from AWS to on-premises when running on on-premises cluster.
* Configuration element `replica-catalog.s3` is now `security`. The following is an example of how to migrate your configuration files to this new version:

_Old configuration file_

	```
	...
	replica-catalog:
	  name: my-replica
	  hive-metastore-uris: thrift://hive.localdomain:9083
	  s3:
	    credential-provider: jceks://hdfs/<hdfs-location-of-jceks-file>/my-credentials.jceks
	...
	```

_New configuration file_

	```
	...
	replica-catalog:
	  name: my-replica
	  hive-metastore-uris: thrift://hive.localdomain:9083
	security:
	  credential-provider: jceks://hdfs/<hdfs-location-of-jceks-file>/my-credentials.jceks
	...
	```

# 2.2.4
* Exit codes based on success or error.

# 2.2.3
* Ignoring params that seem to be added in the replication process.
* Support sending `S3_DIST_CP_BYTES_REPLICATED`/`DIST_CP_BYTES_REPLICATED` metrics to graphite for running (S3)DistCp jobs.

# 2.2.2
* Support for SHH tunneling on source catalog.

# 2.2.1
* Fixes for filter partition generator.

# 2.2.0
* Enabled possibility to generate filter partitions for incremental replication.

# 2.1.3
* Introduction of the transformations API: users can now provide a metadata transformation function for tables, partitions and column statistics.

# 2.1.2
* Fixed issue with deleted paths.

# 2.1.1
* Added some stricter preconditions to the vacuum tool so that data is not unintentionally removed from tables with inconsistent metadata.

# 2.1.0

* Added the 'vacuum' tool for removing data orphaned by a bug in circus-train versions earlier than 2.0.0.
* Moved the 'filter tool' into a 'tools' sub-module.

# 2.0.1

* Fixed issue where housekeeping would fail when two processes deleted the same entry.

# 2.0.0

* SSH tunnels with multiple hops. The property `replica-catalog.metastore-tunnel.user` has been replaced with `replica-catalog.metastore-tunnel.route` and the property `replica-catalog.metastore-tunnel.private-key` had been replaced with `replica-catalog.metastore-tunnel.private-keys`. Refer to _[README.md](README.md)_ for details.
* The executable script has been split to provide both non-RUSH and RUSH executions. If you are not using RUSH the keep using `circus-train.sh` and if you are using RUSH then you can either change your scripts to invoke `circus-train-rush.sh` instead or add the new parameter _rush_ in the first position when invoking `circus-train.sh`.
* Removal of property `graphite.enabled`.
* Improvements and fixes to the housekeeping process that manages old data deletion:
    * Binds `S3AFileSystem` to `s3[n]://` schemes in the tool for housekeeping.
    * Only remove housekeeping entries from the database if:
        * The leaf path described by the record no longer exists AND another sibling exists who can look after the ancestors
        * OR the ancestors of the leaf path no longer exist.
    * Stores the eventId of the deleted path along with the path.
    * If an existing record does not have the previous eventId then reconstruct it from the path (to support legacy data for the time being).

# 1.5.1

* `DistCP` temporary path is now set per task.

