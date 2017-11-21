# Circus Train to Google Storage copier configuration

##  Overview
Copy tables to Google Storage

## Configuration
To enable copying to Google Storage, the user must provide a path to their [Google Credentials](https://cloud.google.com/java/getting-started/authenticate-users) in the configuration under the gcp-security parameter
For more information on configuring Circus Train see [Configuration](../README.md)

#### Example:

    source-catalog:
      name: aws-source
      hive-metastore-uris: thrift://ip-123-45-67-89.us-west-2.compute.internal:9083
    replica-catalog:
      name: gcp-destination
      hive-metastore-uris: thrift://98.765.432.12:9083
    security:
      credential-provider: jceks://hdfs/aws/credential/credentials.jceks
    gcp-security:
      credential-provider: /home/hadoop/.gcp/hcom-circus-train/key/google-credential.json

    table-replications:
      -
        source-table:
          database-name: bdp
          table-name: copy_to_google
        replica-table:
          database-name: bdp
          table-name: copied_to_google
          table-location: gs://my/destination/folder
