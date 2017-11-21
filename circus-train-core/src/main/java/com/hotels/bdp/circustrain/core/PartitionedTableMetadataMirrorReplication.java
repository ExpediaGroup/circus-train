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
import java.util.List;

import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hotels.bdp.circustrain.api.CircusTrainException;
import com.hotels.bdp.circustrain.api.ReplicaLocationManager;
import com.hotels.bdp.circustrain.api.Replication;
import com.hotels.bdp.circustrain.api.SourceLocationManager;
import com.hotels.bdp.circustrain.api.util.DotJoiner;
import com.hotels.bdp.circustrain.core.replica.MetadataMirrorReplicaLocationManager;
import com.hotels.bdp.circustrain.core.replica.Replica;
import com.hotels.bdp.circustrain.core.replica.TableType;
import com.hotels.bdp.circustrain.core.source.Source;

class PartitionedTableMetadataMirrorReplication implements Replication {

  private static final Logger LOG = LoggerFactory.getLogger(PartitionedTableReplication.class);

  private final String database;
  private final String table;
  private final Source source;
  private final Replica replica;
  private final String eventId;
  private final PartitionPredicate partitionPredicate;
  private final String replicaDatabaseName;
  private final String replicaTableName;

  PartitionedTableMetadataMirrorReplication(
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
    eventId = eventIdFactory.newEventId("ctp");
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

      // We expect all partitions to be under the table base path
      SourceLocationManager sourceLocationManager = source.getLocationManager(sourceTable, sourcePartitions, eventId,
          Collections.<String, Object> emptyMap());
      ReplicaLocationManager replicaLocationManager = new MetadataMirrorReplicaLocationManager(sourceLocationManager,
          TableType.PARTITIONED);
      sourceLocationManager.cleanUpLocations();
      replica.updateMetadata(eventId, sourceTableAndStatistics, sourcePartitionsAndStatistics, sourceLocationManager,
          replicaDatabaseName, replicaTableName, replicaLocationManager);
      int partitionsCopied = sourcePartitions.size();

      LOG.info("Metadata mirrored for {} partitions of table {}.{} (no data copied).", partitionsCopied, database,
          table);
    } catch (Throwable t) {
      throw new CircusTrainException("Unable to replicate", t);
    }
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
