# SNS event listener

##  Overview
The SNS event listener is an optional Circus Train component which, if found on the classpath, will push 
replication events as JSON messages onto Amazon Web Services SNS topics.

## Installation
The listener can be activated in a number of ways which are described below.

### Classpath
The jar file produced by this project can be retrieved from [Maven Central](http://mvnrepository.com/artifact/com.hotels/circus-train-aws-sns/) and 
then added to Circus Train's classpath. It is highly recommended that the version of this library and the version of Circus Train are identical. The recommended way 
to make this extension available on the classpath is to store it in a standard location and then add this to the `CIRCUS_TRAIN_CLASSPATH` environment variable 
(e.g. via a startup script):

    export CIRCUS_TRAIN_CLASSPATH=$CIRCUS_TRAIN_CLASSPATH:/your-circus-train-sns-lib-path/*

Another option is to place the jar file in the Circus Train `lib` folder which will automatically pick it up but risks interfering with any Circus Train jobs that do 
not require the extension's functionality.

### Dependency
If you have a project that is using Circus Train as a Maven dependency you could add the XML fragment below 
to your POM file in order to depend on the Circus Train SNS artifact and bundle this onto your application's 
classpath at runtime:

    <dependency>
      <groupId>com.hotels</groupId>
      <artifactId>circus-train-aws-sns</artifactId>
      <version><${cirus-train-sns.version}</version>
      <classifier>all</classifier>
      <exclusions>
        <exclusion>
          <groupId>*</groupId>
          <artifactId>*</artifactId>
        </exclusion>
      </exclusions>
    </dependency>   

  
## Configuration
The SNS listener can be configured by adding something like the following to your Circus Train YAML 
configuration file:

	 # the below is mandatory in order for Circus Train to load the SNS extension
    extension-packages: com.hotels.bdp.circustrain.aws.sns.event 
    # the below now configures the extension
    sns-event-listener:
      region: eu-west-1
      # default topic (will be used if separate start/fail/success topics not configured)
      topic: arn:aws:sns:eu-west-1:aws-account-id:example-sns-topic
      # in this example we're providing a separate topic for failure messages
	  fail-topic: arn:aws:sns:eu-west-1:aws-account-id:example-sns-failure-topic
      # optional subject
      subject: ChooChoo!
      # optional custom headers
      headers:
        pipeline-id: myPipelineId
        kitchen-sink: included

### Configuration Reference

The table below describes all the available configuration options for the SNS listener. 

|Property|Required|Description|
|----|----|----|
|`sns-event-listener.region`|Yes|The region in which the topic is based. Required even though this is contained in the topic ARN.|
|`sns-event-listener.topic`|Maybe|Default topic ARN to which messages are sent. Required if all of: `start-topic`, `success-topic`, and `fail-topic` are not supplied.| 
|`sns-event-listener.start-topic`|No|Topic ARN to which replication 'start' messages should be sent. Defaults to the ARN specified in `sns-event-listener.topic`.|
|`sns-event-listener.success-topic`|No|Topic ARN to which replication 'success' messages should be sent. Defaults to the ARN specified in `sns-event-listener.topic`.|
|`sns-event-listener.fail-topic`|No|Topic ARN to which replication 'failure' messages should be sent. Defaults to the ARN specified in `sns-event-listener.topic`.|
|`sns-event-listener.subject`|No|A subject line that will be set on all SNS messages published.|
|`sns-event-listener.headers`|No|Arbitrary user supplied headers that will be included in all messages published.|

### Credentials
Currently the S3 credentials specified in the JCEKS key store are used to authenticate SNS requests. See the main Circus Train documentation for more information:

      security:
        credential-provider: jceks://hdfs[@namenode:port]/hcom/etl/<team>/conf/aws-<account>.jceks

## JSON Messages
The following table describes all the fields that may be present in the JSON message that is sent to the SNS 
topic.

|Field Name|Type|When present|Description|
|----|----|----|----|
|`protocolVersion`|String|Always|The [semantic version number](https://semver.org/) of the message in MAJOR.MINOR format (i.e. omitting the PATCH version).
|`type`|String Enum Value|Always|One of: START, SUCCESS or FAILURE| 
|`headers`|Map|When configured|Any headers that were set via the `sns-event-listener.headers` configuration property|
|`startTime`|String |Always|Time when the Circus Train job started. Formatted as a date time in ISO8601 format (yyyy-MM-ddTHH:mm:ss.SSSZZ)|
|`endTime`|String |Only when `type` is SUCCESS or FAILURE|Time when the Circus Train job ended. Formatted as a date time in ISO8601 format (yyyy-MM-ddTHH:mm:ss.SSSZZ)|
|`eventId`|String|Always|The Circus Train event id|
|`sourceCatalog`|String|Only when configured|The configured name of the source catalog|
|`replicaCatalog`|String|Only when configured|The configured name of the replicat catalog|
|`sourceTable`|String|Always|The fully qualified name of the source table|
|`replicaTable`|String|Always|The fully qualified name of the replica table|
|`replicaTableLocation`|String|Always|The replicate table location|
|`replicaMetastoreUris`|String|Always|The Hive metastore URIs of the replica Hive Thrift metastore|
|`partitionKeys`|Ordered Map of String->String|Only on successful replication of a partitioned table|A map where the keys are the partition key names and the values are the partition key types, the order output in the JSON matches the order of the values in the `modifiedPartitions` field below|
|`modifiedPartitions`|List of List of Strings|Only on successful replication of a partitioned table|A list containing Lists of string representing the partition key values|
|`bytesReplicated`|Long|Only on replication of `type` SUCCESS of a table containing data|The number of bytes of data replicated|
|`messageTruncated`|Boolean|Only if SNS message exceeded maximum supported length|Will be set to `true` if the generated message exceeded the [maximum supported SNS message length](https://docs.aws.amazon.com/sns/latest/dg/large-payload-raw-message.html). In this case the `modifiedPartitions` will be _empty_ in order to reduce the message size so that this truncated version could be sent|
|`errorMessage`|String|Only on replication of `type` FAILURE|A message describing the cause of the failure|

### Example Messages

#### Replication start
The following shows an example JSON message representing the start of a table replication:

	{
	  "protocolVersion" : "1.2",
	  "type" : "START",
	  "startTime" : "2016-06-01T15:27:38.365Z",
	  "eventId" : "ctp-20160601T152738.363Z-CzbZaYfj",
	  "sourceCatalog" : "sourceCatalogName",
	  "replicaCatalog" : "replicaCatalogName",
	  "sourceTable" : "srcDb.srcTable",
	  "replicaTable" : "replicaDb.replicaTable",
	  "replicaTableLocation" : "s3://bucket/path",
	  "replicaMetastoreUris" : "thrift://host:9083",
	}

#### Replication success for a non-partitioned table
The following shows an example JSON message representing a successful replication of a non-partitioned table:

	{
	  "protocolVersion" : "1.2",
	  "type" : "SUCCESS",
	  "startTime" : "2016-06-01T15:27:38.365Z",
	  "endTime" : "2016-06-01T15:27:39.000Z",
	  "eventId" : "ctp-20160601T152738.363Z-CzbZaYfj",
	  "sourceCatalog" : "sourceCatalogName",
	  "replicaCatalog" : "replicaCatalogName",
	  "sourceTable" : "srcDb.srcTable",
	  "replicaTable" : "replicaDb.replicaTable",
	  "replicaTableLocation" : "s3://bucket/path",
	  "replicaMetastoreUris" : "thrift://host:9083",
	  "bytesReplicated" : 84837488
	}

#### Replication success for a partitioned table
The following shows an example JSON message representing a successful replication of a partitioned table 
and also the usage of custom "pipeline-id" header that was set using the `sns-event-listener.headers` 
configuration value described above:

	{
	  "protocolVersion" : "1.2",
	  "type" : "SUCCESS",
	  "headers" : {
	    "pipeline-id" : "0943879438"
	  },
	  "startTime" : "2016-06-01T15:27:38.365Z",
	  "endTime" : "2016-06-01T15:27:39.000Z",
	  "eventId" : "ctp-20160601T152738.363Z-CzbZaYfj",
	  "sourceCatalog" : "sourceCatalogName",
	  "replicaCatalog" : "replicaCatalogName",
	  "sourceTable" : "srcDb.srcTable",
	  "replicaTable" : "replicaDb.replicaTable",
	  "replicaTableLocation" : "s3://bucket/path",
	  "replicaMetastoreUris" : "thrift://host:9083",
	  "partitionKeys" : {
	    "local_date" : "string",
	    "local_hour" : "int"
	  },
	  "modifiedPartitions" : [ [ "2014-01-01", "0" ], [ "2014-01-01", "1" ] ],
	  "bytesReplicated" : 84837488
	}

#### Replication failure
The following shows an example JSON message representing the failure of a table replication:

	{
	  "protocolVersion" : "1.2",
	  "type" : "FAILURE",
	  "startTime" : "2016-06-01T15:27:38.365Z",
	  "endTime" : "2016-06-01T15:27:39.000Z",
	  "eventId" : "ctp-20160601T152738.363Z-CzbZaYfj",
	  "sourceCatalog" : "sourceCatalogName",
	  "replicaCatalog" : "replicaCatalogName",
	  "sourceTable" : "srcDb.srcTable",
	  "replicaTable" : "replicaDb.replicaTable",
	  "replicaTableLocation" : "s3://bucket/path",
	  "replicaMetastoreUris" : "thrift://host:9083",
	  "errorMessage" : "Connection timed out"
	}

## Limitations
SNS messages have a [size limit of 256KB](https://docs.aws.amazon.com/sns/latest/dg/large-payload-raw-message.html). It is possible that you could exceed this bound if your replication touches a very large number of partitions. In this case your message will be sent with the `modifiedPartitions` set to be empty and the `messageTruncated` field set to `true`. If this occurs you will need to take additional steps to identify which partitions were altered.
