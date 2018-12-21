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

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class SnsMessage {

  final static String PROTOCOL_VERSION = "1.2";

  private static final String protocolVersion = PROTOCOL_VERSION;
  private final SnsMessageType type;
  private final Map<String, String> headers;
  private final String startTime;
  private final String endTime;
  private final String eventId;
  private final String sourceCatalog;
  private final String replicaCatalog;
  private final String sourceTable;
  private final String replicaTable;
  private final String replicaTableLocation;
  private final String replicaMetastoreUris;
  private final LinkedHashMap<String, String> partitionKeys;
  private final List<List<String>> modifiedPartitions;
  private final Long bytesReplicated;
  private final String errorMessage;
  private Boolean messageTruncated = null;

  SnsMessage(
      SnsMessageType type,
      Map<String, String> headers,
      String startTime,
      String endTime,
      String eventId,
      String sourceCatalog,
      String replicaCatalog,
      String replicaMetastoreUris,
      String sourceTable,
      String replicaTable,
      String replicaTableLocation,
      LinkedHashMap<String, String> partitionKeys,
      List<List<String>> modifiedPartitions,
      Long bytesReplicated,
      String errorMessage) {
    this.type = type;
    this.headers = headers;
    this.startTime = startTime;
    this.endTime = endTime;
    this.eventId = eventId;
    this.sourceCatalog = sourceCatalog;
    this.replicaCatalog = replicaCatalog;
    this.sourceTable = sourceTable;
    this.replicaTable = replicaTable;
    this.replicaTableLocation = replicaTableLocation;
    this.replicaMetastoreUris = replicaMetastoreUris;
    this.partitionKeys = partitionKeys;
    this.modifiedPartitions = modifiedPartitions;
    this.bytesReplicated = bytesReplicated;
    this.errorMessage = errorMessage;
  }

  public String getProtocolVersion() {
    return protocolVersion;
  }

  public SnsMessageType getType() {
    return type;
  }

  public Map<String, String> getHeaders() {
    return headers;
  }

  public String getStartTime() {
    return startTime;
  }

  public String getEndTime() {
    return endTime;
  }

  public String getEventId() {
    return eventId;
  }

  public String getSourceCatalog() {
    return sourceCatalog;
  }

  public String getReplicaCatalog() {
    return replicaCatalog;
  }

  public String getSourceTable() {
    return sourceTable;
  }

  public String getReplicaTable() {
    return replicaTable;
  }

  public String getReplicaTableLocation() {
    return replicaTableLocation;
  }

  public String getReplicaMetastoreUris() {
    return replicaMetastoreUris;
  }

  public LinkedHashMap<String, String> getPartitionKeys() {
    return partitionKeys;
  }

  public List<List<String>> getModifiedPartitions() {
    return modifiedPartitions;
  }

  public void clearModifiedPartitions() {
    if (modifiedPartitions != null) {
      modifiedPartitions.clear();
    }
  }

  public Long getBytesReplicated() {
    return bytesReplicated;
  }

  public String getErrorMessage() {
    return errorMessage;
  }

  public Boolean isMessageTruncated() {
    return messageTruncated;
  }

  public void setMessageTruncated(Boolean truncated) {
    messageTruncated = truncated;
  }

}
