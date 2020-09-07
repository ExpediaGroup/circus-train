/**
 * Copyright (C) 2016-2020 Expedia, Inc.
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
package com.hotels.bdp.circustrain.api.copier;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import com.hotels.bdp.circustrain.api.conf.TableReplication;

public final class CopierContext {

  private String eventId;
  private Path sourceBaseLocation;
  private List<Path> sourceSubLocations = ImmutableList.copyOf(Collections.<Path>emptyList());
  private Path replicaLocation;
  private Map<String, Object> copierOptions;
  private TableReplication tableReplication;
  private Table sourceTable;
  private List<Partition> sourcePartitions;

  public CopierContext(
      TableReplication tableReplication,
      String eventId,
      Path sourceBaseLocation,
      List<Path> sourceSubLocations,
      Path replicaLocation,
      Map<String, Object> copierOptions,
      Table sourceTable,
      List<Partition> sourcePartitions) {
    this.tableReplication = tableReplication;
    this.eventId = eventId;
    this.sourceBaseLocation = sourceBaseLocation;
    if (sourceSubLocations != null) {
      this.sourceSubLocations = ImmutableList.copyOf(sourceSubLocations);
    }
    this.replicaLocation = replicaLocation;
    this.copierOptions = ImmutableMap.copyOf(copierOptions);
    this.sourceTable = sourceTable;
    this.sourcePartitions = sourcePartitions;
  }

  public CopierContext(
      TableReplication tableReplication,
      String eventId,
      Path sourceLocation,
      Path replicaLocation,
      Map<String, Object> copierOptions,
      Table sourceTable) {
    this(tableReplication, eventId, sourceLocation, null, replicaLocation, copierOptions, sourceTable, null);
  }

  public CopierContext(
      String eventId,
      Path sourceBaseLocation,
      List<Path> sourceSubLocations,
      Path replicaLocation,
      Map<String, Object> copierOptions,
      Table sourceTable,
      List<Partition> sourcePartitions) {
    this(null, eventId, sourceBaseLocation, sourceSubLocations, replicaLocation, copierOptions, sourceTable,
        sourcePartitions);
  }

  public CopierContext(
      TableReplication tableReplication,
      String eventId,
      Path sourceBaseLocation,
      List<Path> sourceSubLocations,
      Path replicaLocation,
      Map<String, Object> copierOptions) {
    this(tableReplication, eventId, sourceBaseLocation, sourceSubLocations, replicaLocation, copierOptions, null, null);
  }

  public CopierContext(
      TableReplication tableReplication,
      String eventId,
      Path sourceLocation,
      Path replicaLocation,
      Map<String, Object> copierOptions) {
    this(tableReplication, eventId, sourceLocation, null, replicaLocation, copierOptions);
  }

  public CopierContext(
      String eventId,
      Path sourceBaseLocation,
      List<Path> sourceSubLocations,
      Path replicaLocation,
      Map<String, Object> copierOptions) {
    this(null, eventId, sourceBaseLocation, sourceSubLocations, replicaLocation, copierOptions);
  }

  public CopierContext(
      String eventId,
      Path sourceBaseLocation,
      Path replicaLocation,
      Map<String, Object> copierOptions) {
    this(null, eventId, sourceBaseLocation, null, replicaLocation, copierOptions);
  }

  public String getEventId() {
    return eventId;
  }

  public Path getSourceBaseLocation() {
    return sourceBaseLocation;
  }

  public List<Path> getSourceSubLocations() {
    return sourceSubLocations;
  }

  public Path getReplicaLocation() {
    return replicaLocation;
  }

  public Map<String, Object> getCopierOptions() {
    return copierOptions;
  }

  public TableReplication getTableReplication() {
    return tableReplication;
  }

  public Table getSourceTable() {
    return sourceTable;
  }

  public List<Partition> getSourcePartitions() {
    return sourcePartitions;
  }
}
