/**
 * Copyright (C) 2016-2019 Expedia, Inc.
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
package com.hotels.bdp.circustrain.aws.sns.event;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.collect.Lists;

/**
 * This isn't intended as a proper unit test, but is an easy way to generate the JSON String version of an SnsMessage
 * for visual inspection (e.g. during development, to generate samples for documentation etc.)
 */
public class SnsMessageTest {

  // set below to true if you want to print messages
  private final static boolean PRINT_MESSAGE = false;

  private void printMessage(SnsMessage message) throws JsonProcessingException {
    if (PRINT_MESSAGE) {
      ObjectMapper mapper = new ObjectMapper();
      mapper.setSerializationInclusion(Include.NON_NULL);
      ObjectWriter objectWriter = mapper.writerWithDefaultPrettyPrinter();
      String jsonMessage = objectWriter.writeValueAsString(message);
      System.out.println(jsonMessage);
    }
  }

  @Test
  public void partitionTableAllFieldsSuccess() throws JsonProcessingException {
    SnsMessageType type = SnsMessageType.SUCCESS;
    Map<String, String> headers = new HashMap<>();
    headers.put("pipeline-id", "0943879438");
    String startTime = "2016-06-01T15:27:38.365Z";
    String endTime = "2016-06-01T15:27:39.000Z";
    String eventId = "ctp-20160601T152738.363Z-CzbZaYfj";
    String sourceCatalog = "sourceCatalogName";
    String replicaCatalog = "replicaCatalogName";
    String sourceTable = "srcDb.srcTable";
    String replicaTable = "replicaDb.replicaTable";
    String replicaTableLocation = "s3://bucket/path";
    String replicaMetastoreUris = "thrift://host:9083";
    List<String> partition1 = Lists.newArrayList("2014-01-01", "0");
    List<String> partition2 = Lists.newArrayList("2014-01-01", "1");
    List<List<String>> modifiedPartitions = Lists.newArrayList(partition1, partition2);
    long bytesReplicated = 84837488L;
    String errorMessage = null;
    LinkedHashMap<String, String> partitionKeys = new LinkedHashMap<>();
    partitionKeys.put("local_date", "string");
    partitionKeys.put("local_hour", "int");
    SnsMessage message = new SnsMessage(type, headers, startTime, endTime, eventId, sourceCatalog, replicaCatalog,
        replicaMetastoreUris, sourceTable, replicaTable, replicaTableLocation, partitionKeys, modifiedPartitions,
        bytesReplicated, errorMessage);
    printMessage(message);
  }

  @Test
  public void partitionTableStart() throws JsonProcessingException {
    SnsMessageType type = SnsMessageType.START;
    Map<String, String> headers = null;
    String startTime = "2016-06-01T15:27:38.365Z";
    String endTime = null;
    String eventId = "ctp-20160601T152738.363Z-CzbZaYfj";
    String sourceCatalog = "sourceCatalogName";
    String replicaCatalog = "replicaCatalogName";
    String sourceTable = "srcDb.srcTable";
    String replicaTable = "replicaDb.replicaTable";
    String replicaTableLocation = "s3://bucket/path";
    String replicaMetastoreUris = "thrift://host:9083";
    List<List<String>> modifiedPartitions = null;
    long bytesReplicated = 0L;
    String errorMessage = null;
    LinkedHashMap<String, String> partitionKeys = null;
    SnsMessage message = new SnsMessage(type, headers, startTime, endTime, eventId, sourceCatalog, replicaCatalog,
        replicaMetastoreUris, sourceTable, replicaTable, replicaTableLocation, partitionKeys, modifiedPartitions,
        bytesReplicated, errorMessage);

    printMessage(message);
  }

  @Test
  public void partitionTableFailure() throws JsonProcessingException {
    SnsMessageType type = SnsMessageType.FAILURE;
    Map<String, String> headers = null;
    String startTime = "2016-06-01T15:27:38.365Z";
    String endTime = "2016-06-01T15:27:39.000Z";
    String eventId = "ctp-20160601T152738.363Z-CzbZaYfj";
    String sourceCatalog = "sourceCatalogName";
    String replicaCatalog = "replicaCatalogName";
    String sourceTable = "srcDb.srcTable";
    String replicaTable = "replicaDb.replicaTable";
    String replicaTableLocation = "s3://bucket/path";
    String replicaMetastoreUris = "thrift://host:9083";
    List<List<String>> modifiedPartitions = null;
    long bytesReplicated = 0;
    String errorMessage = "error message";
    LinkedHashMap<String, String> partitionKeys = null;
    SnsMessage message = new SnsMessage(type, headers, startTime, endTime, eventId, sourceCatalog, replicaCatalog,
        replicaMetastoreUris, sourceTable, replicaTable, replicaTableLocation, partitionKeys, modifiedPartitions,
        bytesReplicated, errorMessage);

    printMessage(message);
  }

  @Test
  public void nonPartitionTableSuccess() throws JsonProcessingException {
    SnsMessageType type = SnsMessageType.SUCCESS;
    Map<String, String> headers = null;
    String startTime = "2016-06-01T15:27:38.365Z";
    String endTime = "2016-06-01T15:27:39.000Z";
    String eventId = "ctp-20160601T152738.363Z-CzbZaYfj";
    String sourceCatalog = "sourceCatalogName";
    String replicaCatalog = "replicaCatalogName";
    String sourceTable = "srcDb.srcTable";
    String replicaTable = "replicaDb.replicaTable";
    String replicaTableLocation = "s3://bucket/path";
    String replicaMetastoreUris = "thrift://host:9083";
    List<List<String>> modifiedPartitions = null;
    long bytesReplicated = 84837488L;
    String errorMessage = null;
    LinkedHashMap<String, String> partitionKeys = null;
    SnsMessage message = new SnsMessage(type, headers, startTime, endTime, eventId, sourceCatalog, replicaCatalog,
        replicaMetastoreUris, sourceTable, replicaTable, replicaTableLocation, partitionKeys, modifiedPartitions,
        bytesReplicated, errorMessage);

    printMessage(message);
  }

  @Test
  public void clearPartitions() throws JsonProcessingException {
    List<String> partition1 = Lists.newArrayList("2014-01-01", "1");
    List<List<String>> modifiedPartitions = Lists.newArrayList();
    modifiedPartitions.add(partition1);
    SnsMessage message = new SnsMessage(null, null, null, null, null, null, null,
        null, null, null, null, null, modifiedPartitions,
        0L, null);
    assertThat(message.getModifiedPartitions().size(), is(1));
    message.clearModifiedPartitions();
    assertThat(message.getModifiedPartitions().size(), is(0));
  }

  @Test
  public void clearNullPartitions() throws JsonProcessingException {
    SnsMessage message = new SnsMessage(null, null, null, null, null, null, null, null, null, null, null, null, null,
        0L, null);
    message.clearModifiedPartitions();
  }

}
