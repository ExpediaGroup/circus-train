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
package com.hotels.bdp.circustrain.core;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;

import com.hotels.bdp.circustrain.api.CircusTrainException;
import com.hotels.bdp.circustrain.api.ReplicaLocationManager;
import com.hotels.bdp.circustrain.api.Replication;
import com.hotels.bdp.circustrain.api.SourceLocationManager;
import com.hotels.bdp.circustrain.api.metastore.CloseableMetaStoreClient;
import com.hotels.bdp.circustrain.api.util.DotJoiner;
import com.hotels.bdp.circustrain.core.conf.ReplicationMode;
import com.hotels.bdp.circustrain.core.replica.InvalidReplicationModeException;
import com.hotels.bdp.circustrain.core.replica.MetadataUpdateReplicaLocationManager;
import com.hotels.bdp.circustrain.core.replica.Replica;
import com.hotels.bdp.circustrain.core.replica.TableType;
import com.hotels.bdp.circustrain.core.source.Source;

public class PartitionedTableMetadataUpdateReplication implements Replication {

  private final static Logger LOG = LoggerFactory.getLogger(PartitionedTableMetadataUpdateReplication.class);

  private final String eventId;
  private final String database;
  private final String table;
  private final PartitionPredicate partitionPredicate;
  private final Source source;
  private final Replica replica;
  private final String replicaDatabaseName;
  private final String replicaTableName;

  public PartitionedTableMetadataUpdateReplication(
      String database,
      String table,
      PartitionPredicate partitionPredicate,
      Source source,
      Replica replica,
      EventIdFactory eventIdFactory,
      String replicaDatabaseName,
      String replicaTableName) {
    this.database = database;
    this.table = table;
    this.partitionPredicate = partitionPredicate;
    this.source = source;
    this.replica = replica;
    this.replicaDatabaseName = replicaDatabaseName;
    this.replicaTableName = replicaTableName;
    eventId = eventIdFactory.newEventId("ctt");
  }

  @Override
  public void replicate() throws CircusTrainException {
    try {
      TableAndStatistics sourceTableAndStatistics = source.getTableAndStatistics(database, table);
      Table sourceTable = sourceTableAndStatistics.getTable();

      PartitionsAndStatistics sourcePartitionsAndStatistics = source.getPartitions(sourceTable,
          partitionPredicate.getPartitionPredicate(), partitionPredicate.getPartitionPredicateLimit());
      List<Partition> sourcePartitions = sourcePartitionsAndStatistics.getPartitions();
      if (sourcePartitions.isEmpty()) {
        LOG.info("No matching partitions found on table {}.{} with predicate {}; Nothing to do.", database, table,
            partitionPredicate);
        return;
      }

      replica.validateReplicaTable(replicaDatabaseName, replicaTableName);
      try (CloseableMetaStoreClient client = replica.getMetaStoreClientSupplier().get()) {
        String previousLocation = getPreviousLocation(client);
        PartitionsAndStatistics sourcePartitionsAndStatisticsThatWereReplicated = filterOnReplicatedPartitions(client,
            sourcePartitionsAndStatistics, sourceTable.getPartitionKeys());
        SourceLocationManager sourceLocationManager = source.getLocationManager(sourceTable,
            sourcePartitionsAndStatisticsThatWereReplicated.getPartitions(), eventId,
            Collections.<String, Object> emptyMap());
        ReplicaLocationManager replicaLocationManager = new MetadataUpdateReplicaLocationManager(client,
            TableType.PARTITIONED, previousLocation, replicaDatabaseName, replicaTableName);

        replica.updateMetadata(eventId, sourceTableAndStatistics, sourcePartitionsAndStatisticsThatWereReplicated,
            sourceLocationManager, replicaDatabaseName, replicaTableName, replicaLocationManager);
      }
      int partitionsCopied = sourcePartitions.size();

      LOG.info("Metadata updated for {} partitions of table {}.{}. (no data copied)", partitionsCopied, database,
          table);

    } catch (Throwable t) {
      throw new CircusTrainException("Unable to replicate", t);
    }
  }

  private PartitionsAndStatistics filterOnReplicatedPartitions(
      CloseableMetaStoreClient replicaClient,
      PartitionsAndStatistics sourcePartitionsAndStatistics,
      List<FieldSchema> partitionKeys)
    throws TException {
    Map<Partition, ColumnStatistics> statisticsByPartition = new LinkedHashMap<>();
    for (Partition partition : sourcePartitionsAndStatistics.getPartitions()) {
      try {
        replicaClient.getPartition(replicaDatabaseName, replicaTableName, partition.getValues());
        statisticsByPartition.put(partition, sourcePartitionsAndStatistics.getStatisticsForPartition(partition));
      } catch (NoSuchObjectException e) {
        LOG.debug(
            "Partition {} not found on replica, skipping metadata update (Set ReplicationMode.FULL to replicate missing partitions and data)",
            partition.getValues());
      }
    }
    return new PartitionsAndStatistics(partitionKeys, statisticsByPartition);
  }

  private String getPreviousLocation(CloseableMetaStoreClient replicaClient) {
    Optional<Table> previousTable = replica.getTable(replicaClient, replicaDatabaseName, replicaTableName);
    if (!previousTable.isPresent()) {
      throw new InvalidReplicationModeException("Trying a "
          + ReplicationMode.METADATA_UPDATE.name()
          + " on a table that wasn't replicated before. This is not possible, rerun with a different table name or change the replication mode to "
          + ReplicationMode.FULL.name()
          + ".");

    }
    return previousTable.get().getSd().getLocation();
  }

  @Override
  public String name() {
    return DotJoiner.join(database, table);
  }

  @Override
  public String getEventId() {
    return eventId;
  }

}
