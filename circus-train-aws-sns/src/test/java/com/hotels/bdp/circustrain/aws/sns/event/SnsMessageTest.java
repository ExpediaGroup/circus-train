/**
 * Copyright (C) 2016-2018 Expedia Inc.
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.collect.Lists;

public class SnsMessageTest {

  /**
   * This isn't intended as a proper unit test, but is an easy way to generate the JSON String version of an SnsMessage
   * for visual inspection (e.g. during development, to generate samples for documentation etc.)
   */
  @Test
  public void displayMessage() throws JsonProcessingException {
    ObjectMapper mapper = new ObjectMapper();
    mapper.setSerializationInclusion(Include.NON_NULL);
    ObjectWriter objectWriter = mapper.writerWithDefaultPrettyPrinter();
    SnsMessageType type = SnsMessageType.FAILURE;
    Map<String, String> headers = new HashMap<>();
    headers.put("pipeline-id", "0943879438");
    String startTime = "2016-06-01T15:27:38.365Z";
    String endTime = "2016-06-01T15:27:39.000Z";
    String eventId = "ctp-20160601T152738.363Z-CzbZaYfj";
    String sourceCatalog = "sourceCatalogName";
    String replicaCatalog = "replicaCatalogName";
    String sourceTable = "srcDb.srcTable";
    String replicaTable = "replicaDb.replicaTable";
    //TODO: need to decide if we want to keep this format for partitions
    //or instead to move to name, value pairs for each partition key
    List<String> partition1 = Lists.newArrayList("2014-01-01", "0");
    List<String> partition2 = Lists.newArrayList("2014-01-01", "1");
    List<List<String>> modifiedPartitions = new ArrayList<>();
    modifiedPartitions.add(partition1);
    modifiedPartitions.add(partition2);
    long bytesReplicated = 84837488L;
    String errorMessage = "error message";
    SnsMessage message = new SnsMessage(type, headers, startTime, endTime, eventId, sourceCatalog, replicaCatalog,
        sourceTable, replicaTable, modifiedPartitions, bytesReplicated, errorMessage);
    String jsonMessage = objectWriter.writeValueAsString(message);
    System.out.println(jsonMessage);
  }

}
