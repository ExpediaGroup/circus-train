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
package com.hotels.bdp.circustrain.core;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hotels.bdp.circustrain.api.CircusTrainException;
import com.hotels.bdp.circustrain.api.ReplicaLocationManager;
import com.hotels.bdp.circustrain.api.Replication;
import com.hotels.bdp.circustrain.api.SourceLocationManager;
import com.hotels.bdp.circustrain.api.conf.DataManipulationClient;
import com.hotels.bdp.circustrain.api.copier.Copier;
import com.hotels.bdp.circustrain.api.copier.CopierFactory;
import com.hotels.bdp.circustrain.api.copier.CopierFactoryManager;
import com.hotels.bdp.circustrain.api.event.CopierListener;
import com.hotels.bdp.circustrain.api.metrics.Metrics;
import com.hotels.bdp.circustrain.api.util.DotJoiner;
import com.hotels.bdp.circustrain.core.replica.Replica;
import com.hotels.bdp.circustrain.core.replica.TableType;
import com.hotels.bdp.circustrain.core.source.Source;

class PartitionedTableReplication implements Replication {

  private static final Logger LOG = LoggerFactory.getLogger(PartitionedTableReplication.class);

  private final String database;
  private final String table;
  private final Source source;
  private final Replica replica;
  private final String eventId;
  private final CopierFactoryManager copierFactoryManager;
  private final PartitionPredicate partitionPredicate;
  private final String targetTableLocation;
  private final String replicaDatabaseName;
  private final String replicaTableName;
  private Metrics metrics = Metrics.NULL_VALUE;
  private final Map<String, Object> copierOptions;
  private final CopierListener copierListener;

  PartitionedTableReplication(
      String database,
      String table,
      PartitionPredicate partitionPredicate,
      Source source,
      Replica replica,
      CopierFactoryManager copierFactoryManager,
      EventIdFactory eventIdFactory,
      String targetTableLocation,
      String replicaDatabaseName,
      String replicaTableName,
      Map<String, Object> copierOptions,
      CopierListener copierListener) {
    this.database = database;
    this.table = table;
    this.partitionPredicate = partitionPredicate;
    this.source = source;
    this.replica = replica;
    this.copierFactoryManager = copierFactoryManager;
    this.targetTableLocation = targetTableLocation;
    this.replicaDatabaseName = replicaDatabaseName;
    this.replicaTableName = replicaTableName;
    this.copierOptions = copierOptions;
    this.copierListener = copierListener;
    eventId = eventIdFactory.newEventId(EventIdPrefix.CIRCUS_TRAIN_PARTITIONED_TABLE.getPrefix());
  }

  @Override
  public void replicate() throws CircusTrainException {
    try {
      TableAndStatistics sourceTableAndStatistics = source.getTableAndStatistics(database, table);
      Table sourceTable = sourceTableAndStatistics.getTable();

      PartitionsAndStatistics sourcePartitionsAndStatistics = source
          .getPartitions(sourceTable, partitionPredicate.getPartitionPredicate(),
              partitionPredicate.getPartitionPredicateLimit());
      List<Partition> sourcePartitions = sourcePartitionsAndStatistics.getPartitions();

      replica.validateReplicaTable(replicaDatabaseName, replicaTableName);

      // We expect all partitions to be under the table base path
      SourceLocationManager sourceLocationManager = source
          .getLocationManager(sourceTable, sourcePartitions, eventId, copierOptions);
      Path sourceBaseLocation = sourceLocationManager.getTableLocation();
      List<Path> sourceSubLocations = sourceLocationManager.getPartitionLocations();

      ReplicaLocationManager replicaLocationManager = replica
          .getLocationManager(TableType.PARTITIONED, targetTableLocation, eventId, sourceLocationManager);
      Path replicaPartitionBaseLocation = replicaLocationManager.getPartitionBaseLocation();

      if (sourcePartitions.isEmpty()) {
        LOG.debug("Update table {}.{} metadata only", database, table);
        replica
            .updateMetadata(eventId, sourceTableAndStatistics, replicaDatabaseName, replicaTableName,
                replicaLocationManager);
        LOG
            .info("No matching partitions found on table {}.{} with predicate {}."
                + " Table metadata updated, no partitions were updated.", database, table, partitionPredicate);
      } else {
        CopierFactory copierFactory = copierFactoryManager
            .getCopierFactory(sourceBaseLocation, replicaPartitionBaseLocation, copierOptions);
        Copier copier = copierFactory
            .newInstance(eventId, sourceBaseLocation, sourceSubLocations, replicaPartitionBaseLocation, copierOptions);
        copierListener.copierStart(copier.getClass().getName());
        try {

          // after the copy happens the transfer manager (for s3s3copier) gets shutdown, which I assume will also shut
          // down the client
          DataManipulationClient dataManipulationClient = copier.getClient();
          replica.checkIfReplicaCleanupRequired(replicaDatabaseName, replicaTableName, dataManipulationClient);

          System.out.println(">>>>>>>>>>>>>> Cleaned up replica, starting copy...");

          metrics = copier.copy();
        } finally {
          copierListener.copierEnd(metrics);
        }

        sourceLocationManager.cleanUpLocations();


        replica
            .updateMetadata(eventId, sourceTableAndStatistics, sourcePartitionsAndStatistics, replicaDatabaseName,
                replicaTableName, replicaLocationManager);
        replicaLocationManager.cleanUpLocations();

        int partitionsCopied = sourcePartitions.size();
        LOG.info("Replicated {} partitions of table {}.{}.", partitionsCopied, database, table);
      }
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
