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
package com.hotels.bdp.circustrain.api.conf;

import java.util.HashMap;
import java.util.Map;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

import com.hotels.bdp.circustrain.api.validation.constraints.TableReplicationFullReplicationModeConstraint;

@TableReplicationFullReplicationModeConstraint
public class TableReplication {

  private @Valid @NotNull SourceTable sourceTable;
  private @Valid @NotNull ReplicaTable replicaTable;
  private Map<String, Object> copierOptions;
  private Map<String, Object> transformOptions = new HashMap<>();
  private short partitionIteratorBatchSize = (short) 1000;
  private short partitionFetcherBufferSize = (short) 1000;
  private @NotNull ReplicationMode replicationMode = ReplicationMode.FULL;
  // Only relevant to view replications
  private Map<String, String> tableMappings;

  public SourceTable getSourceTable() {
    return sourceTable;
  }

  public void setSourceTable(SourceTable sourceTable) {
    this.sourceTable = sourceTable;
  }

  public ReplicaTable getReplicaTable() {
    return replicaTable;
  }

  public void setReplicaTable(ReplicaTable replicaTable) {
    this.replicaTable = replicaTable;
  }

  public Map<String, Object> getCopierOptions() {
    return copierOptions;
  }

  public void setCopierOptions(Map<String, Object> copierOptions) {
    this.copierOptions = copierOptions;
  }

  public Map<String, Object> getTransformOptions() {
    return transformOptions;
  }

  public void setTransformOptions(Map<String, Object> transformOptions) {
    this.transformOptions = transformOptions;
  }

  public String getReplicaDatabaseName() {
    SourceTable sourceTable = getSourceTable();
    ReplicaTable replicaTable = getReplicaTable();
    String databaseName = replicaTable.getDatabaseName() != null ? replicaTable.getDatabaseName()
        : sourceTable.getDatabaseName();
    return databaseName.toLowerCase();
  }

  public String getReplicaTableName() {
    SourceTable sourceTable = getSourceTable();
    ReplicaTable replicaTable = getReplicaTable();
    String tableNameName = replicaTable.getTableName() != null ? replicaTable.getTableName()
        : sourceTable.getTableName();
    return tableNameName.toLowerCase();
  }

  public String getQualifiedReplicaName() {
    return getReplicaDatabaseName() + "." + getReplicaTableName();
  }

  public short getPartitionIteratorBatchSize() {
    return partitionIteratorBatchSize;
  }

  public void setPartitionIteratorBatchSize(short partitionIteratorBatchSize) {
    this.partitionIteratorBatchSize = partitionIteratorBatchSize;
  }

  public short getPartitionFetcherBufferSize() {
    return partitionFetcherBufferSize;
  }

  public void setPartitionFetcherBufferSize(short partitionFetcherBufferSize) {
    this.partitionFetcherBufferSize = partitionFetcherBufferSize;
  }

  public ReplicationMode getReplicationMode() {
    return replicationMode;
  }

  public void setReplicationMode(ReplicationMode replicationMode) {
    this.replicationMode = replicationMode;
  }

  public Map<String, String> getTableMappings() {
    return tableMappings;
  }

  public void setTableMappings(Map<String, String> tableMappings) {
    this.tableMappings = tableMappings;
  }

}
