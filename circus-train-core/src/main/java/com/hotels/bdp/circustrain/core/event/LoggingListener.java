/**
 * Copyright (C) 2016-2019 Expedia Inc.
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
package com.hotels.bdp.circustrain.core.event;

import java.net.URI;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;

import com.hotels.bdp.circustrain.api.CompletionCode;
import com.hotels.bdp.circustrain.api.event.CopierListener;
import com.hotels.bdp.circustrain.api.event.EventPartitions;
import com.hotels.bdp.circustrain.api.event.EventReplicaCatalog;
import com.hotels.bdp.circustrain.api.event.EventSourceCatalog;
import com.hotels.bdp.circustrain.api.event.EventTable;
import com.hotels.bdp.circustrain.api.event.EventTableReplication;
import com.hotels.bdp.circustrain.api.event.LocomotiveListener;
import com.hotels.bdp.circustrain.api.event.ReplicaCatalogListener;
import com.hotels.bdp.circustrain.api.event.SourceCatalogListener;
import com.hotels.bdp.circustrain.api.event.TableReplicationListener;
import com.hotels.bdp.circustrain.api.metrics.Metrics;

@Component
public class LoggingListener implements TableReplicationListener, LocomotiveListener, SourceCatalogListener,
    ReplicaCatalogListener, CopierListener {

  /* String literal here so that we don't inadvertently lose logging due to refactoring. */
  static final Logger LOG = LoggerFactory
      .getLogger("com.hotels.bdp.circustrain.core.event.LoggingListener:REPLICATION_EVENTS");

  private EventSourceCatalog sourceCatalog;
  private EventReplicaCatalog replicaCatalog;
  private ReplicationState replicationState = new ReplicationState();

  @VisibleForTesting
  static class ReplicationState {
    private List<String> partitionKeys;
    private int partitionsAltered;
    private long bytesReplicated;

    public List<String> getPartitionKeys() {
      return partitionKeys;
    }

    public void setPartitionKeys(List<String> partitionKeys) {
      this.partitionKeys = partitionKeys;
    }

    public int getPartitionsAltered() {
      return partitionsAltered;
    }

    public void setPartitionsAltered(int partitionsAltered) {
      this.partitionsAltered = partitionsAltered;
    }

    public long getBytesReplicated() {
      return bytesReplicated;
    }

    public void setBytesReplicated(long bytesReplicated) {
      this.bytesReplicated = bytesReplicated;
    }

  }

  @Override
  public void tableReplicationStart(EventTableReplication tableReplication, String eventId) {
    replicationState = new ReplicationState();
    if (catalogsNotNull()) {
      LOG
          .info("[{}] Attempting to replicate '{}:{}' to '{}:{}'", eventId, sourceCatalog.getName(),
              tableReplication.getSourceTable().getQualifiedName(), replicaCatalog.getName(),
              tableReplication.getQualifiedReplicaName());
    }
  }

  @Override
  public void tableReplicationSuccess(EventTableReplication tableReplication, String eventId) {
    String amount = transferAmount(replicationState.getPartitionKeys(), replicationState.getPartitionsAltered());
    if (catalogsNotNull()) {
      LOG
          .info("[{}] Successfully replicated {} of '{}:{}' to '{}:{}' ({} bytes)", eventId, amount,
              sourceCatalog.getName(), tableReplication.getSourceTable().getQualifiedName(), replicaCatalog.getName(),
              tableReplication.getQualifiedReplicaName(), replicationState.getBytesReplicated());
    }
  }

  @Override
  public void tableReplicationFailure(EventTableReplication tableReplication, String eventId, Throwable t) {
    if (catalogsNotNull()) {
      LOG
          .error("[{}] Failed to replicate '{}:{}' to '{}:{}' with error '{}'", eventId, sourceCatalog.getName(),
              tableReplication.getSourceTable().getQualifiedName(), replicaCatalog.getName(),
              tableReplication.getQualifiedReplicaName(), t.getMessage());
    }
  }

  private boolean catalogsNotNull() {
    return sourceCatalog != null && replicaCatalog != null;
  }

  private static String transferAmount(List<String> partitionKeys, int partitionsAltered) {
    return partitionKeys == null || partitionKeys.isEmpty() ? "all"
        : Integer.toString(partitionsAltered) + " partitions";
  }

  @Override
  public void circusTrainStartUp(String[] args, EventSourceCatalog sourceCatalog, EventReplicaCatalog replicaCatalog) {
    this.sourceCatalog = sourceCatalog;
    this.replicaCatalog = replicaCatalog;
  }

  @Override
  public void resolvedMetaStoreSourceTable(EventTable table) {
    replicationState.setPartitionKeys(table.getPartitionKeys());
  }

  @Override
  public void partitionsToCreate(EventPartitions partitions) {
    replicationState
        .setPartitionsAltered(replicationState.getPartitionsAltered() + partitions.getEventPartitions().size());
  }

  @Override
  public void partitionsToAlter(EventPartitions partitions) {
    replicationState
        .setPartitionsAltered(replicationState.getPartitionsAltered() + partitions.getEventPartitions().size());
  }

  @Override
  public void copierEnd(Metrics metrics) {
    replicationState.setBytesReplicated(metrics.getBytesReplicated());
  }

  @Override
  public void circusTrainShutDown(CompletionCode completionCode, Map<String, Long> metrics) {}

  @Override
  public void resolvedSourcePartitions(EventPartitions partitions) {}

  @Override
  public void resolvedSourceLocation(URI location) {}

  @Override
  public void resolvedReplicaLocation(URI location) {}

  @Override
  public void existingReplicaPartitions(EventPartitions partitions) {}

  @Override
  public void deprecatedReplicaLocations(List<URI> locations) {}

  @Override
  public void copierStart(String copierImplementation) {}

  @VisibleForTesting
  ReplicationState getReplicationState() {
    return replicationState;
  }

}
