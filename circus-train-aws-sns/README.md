# SNS event listener

##  Overview
Push replication events as JSON onto Amazon Web Services SNS topics.

## Installation
Add the connector as a dependency to your Circus Train project or put in on the Circus Train classpath:
### Dependency
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
### Classpath
    export CIRCUS_TRAIN_CLASSPATH=$CIRCUS_TRAIN_CLASSPATH:/your-circus-train-sns-lib-path/*
   
## Configuration
Add the following to your Circus Train YAML file:

    sns-event-listener:
      region: eu-west-1
      # default topic (conditionally optional)
      topic: MyTopic
      # optional override topic (also for start and success)
      fail-topic: MyFailTopic
      # Optional subject
      subject: ChooChoo!
      # Optional custom headers
      headers:
        pipeline-id: myPipelineId
        kitchen-sink: included

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

## Example message

	{
	   "protocolVersion" : "1.0",
	   "type" : "FAILURE",
	   "headers" : {
	      "pipeline-id" : "0943879438"
	   },
	   "eventId" : "ctp-20160601T152738.363Z-CzbZaYfj",
	   "startTime" : "2016-06-01T15:27:38.365Z",
	   "sourceCatalog" : "sourceCatalogName",
	   "sourceTable" : "srcDb.srcTable",
	   "replicaCatalog" : "replicaCatalogName",
	   "replicaTable" : "replicaDb.replicaTable",
	   "bytesReplicated" : 84837488,
	   "modifiedPartitions" : [
	      ["2014-01-01", "0"],
	      ["2014-01-01", "1"]
	   ],
	   "endTime" : "2016-06-01T15:27:38.365Z",
	   "errorMessage" : "error message"
	}

## Limitations
SNS messages have a size limit of 256KB. It is possible that you could exceed this bound if your replication touches a very large number of partitions. In this case your message will not be sent and instead logged with a warning.