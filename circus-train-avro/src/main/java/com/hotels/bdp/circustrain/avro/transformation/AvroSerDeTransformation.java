/**
 * Copyright (C) 2016-2017 Expedia Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hotels.bdp.circustrain.avro.transformation;

import java.io.IOException;
import java.nio.file.Files;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;

import com.hotels.bdp.circustrain.api.Modules;

@Profile({ Modules.REPLICATION })
@Component
class AvroSerDeTransformation {

  private static final Logger LOG = LoggerFactory.getLogger(AvroSerDeTransformation.class);
  private static final String AVRO_SCHEMA_URL_PARAMETER = "avro.schema.url";

  private final HiveConf sourceHiveConf;
  private final HiveConf replicaHiveConf;

  private java.nio.file.Path temporaryDirectory;

  @Autowired
  AvroSerDeTransformation(HiveConf sourceHiveConf, HiveConf replicaHiveConf) {
    this.sourceHiveConf = sourceHiveConf;
    this.replicaHiveConf = replicaHiveConf;
  }

  private boolean copy(String source, String destination) throws IOException {
    if (source == null || destination == null) {
      return false;
    }

    if (temporaryDirectory == null) {
      temporaryDirectory = Files.createTempDirectory("avro-schema-download-folder");
      temporaryDirectory.toFile().deleteOnExit();
    }

    FileSystem sourceFileSystem = new Path(source).getFileSystem(sourceHiveConf);
    String tempPath = temporaryDirectory.toString();
    sourceFileSystem.copyToLocalFile(false, new Path(source), new Path(tempPath));

    FileSystem destinationFileSystem = new Path(destination).getFileSystem(replicaHiveConf);
    destinationFileSystem.copyFromLocalFile(true, new Path(tempPath), new Path(destination));
    LOG.info("Avro schema has been copied from '{}' to '{}'", source, destination);

    return destinationFileSystem.exists(new Path(destination));
  }

  @VisibleForTesting
  String getAvroSchemaFileName(String avroSchemaSource) {
    if (avroSchemaSource == null) {
      return "";
    }
    return avroSchemaSource.substring(avroSchemaSource.lastIndexOf("/") + 1, avroSchemaSource.length());
  }

  @VisibleForTesting
  String addTrailingSlash(String str) {
    if (str != null && str.charAt(str.length() - 1) != '/') {
      str += "/";
    }
    return str;
  }

  Table apply(Table table, String avroSchemaDestination, String eventId) throws Exception {
    if (avroSchemaDestination == null) {
      return table;
    }

    avroSchemaDestination = addTrailingSlash(avroSchemaDestination);
    avroSchemaDestination += eventId;

    String avroSchemaSource = table.getParameters().get(AVRO_SCHEMA_URL_PARAMETER);
    copy(avroSchemaSource, avroSchemaDestination);

    table.putToParameters(AVRO_SCHEMA_URL_PARAMETER,
        avroSchemaDestination + "/" + getAvroSchemaFileName(avroSchemaSource));
    LOG.info("Avro SerDe transformation has been applied to table '{}'", table.getTableName());
    return table;
  }

  Partition apply(Partition partition, String avroSchemaDestination, String eventId) throws Exception {
    if (avroSchemaDestination == null) {
      return partition;
    }

    avroSchemaDestination = addTrailingSlash(avroSchemaDestination);
    avroSchemaDestination += eventId;

    String avroSchemaSource = partition.getParameters().get(AVRO_SCHEMA_URL_PARAMETER);
    copy(avroSchemaSource, avroSchemaDestination);

    partition.putToParameters(AVRO_SCHEMA_URL_PARAMETER,
        avroSchemaDestination + "/" + getAvroSchemaFileName(avroSchemaSource));
    LOG.info("Avro SerDe transformation has been applied to partition '{}'", partition.toString());
    return partition;
  }
}
