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
package com.hotels.bdp.circustrain.core.replica;

import static com.google.common.base.Strings.nullToEmpty;

import static com.hotels.bdp.circustrain.api.CircusTrainTableParameter.REPLICATION_EVENT;
import static com.hotels.bdp.circustrain.api.CircusTrainTableParameter.REPLICATION_MODE;
import static com.hotels.bdp.circustrain.api.conf.ReplicationMode.FULL;
import static com.hotels.bdp.circustrain.api.conf.ReplicationMode.FULL_OVERWRITE;
import static com.hotels.bdp.circustrain.api.conf.ReplicationMode.METADATA_MIRROR;
import static com.hotels.bdp.circustrain.api.conf.ReplicationMode.METADATA_UPDATE;
import static com.hotels.hcommon.hive.metastore.util.LocationUtils.locationAsPath;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SetPartitionsStatsRequest;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Enums;
import com.google.common.base.Optional;
import com.google.common.base.Supplier;
import com.google.common.collect.Lists;

import com.hotels.bdp.circustrain.api.CircusTrainException;
import com.hotels.bdp.circustrain.api.ReplicaLocationManager;
import com.hotels.bdp.circustrain.api.SourceLocationManager;
import com.hotels.bdp.circustrain.api.conf.ReplicaCatalog;
import com.hotels.bdp.circustrain.api.conf.ReplicationMode;
import com.hotels.bdp.circustrain.api.conf.TableReplication;
import com.hotels.bdp.circustrain.api.event.ReplicaCatalogListener;
import com.hotels.bdp.circustrain.api.listener.HousekeepingListener;
import com.hotels.bdp.circustrain.core.HiveEndpoint;
import com.hotels.bdp.circustrain.core.PartitionsAndStatistics;
import com.hotels.bdp.circustrain.core.TableAndStatistics;
import com.hotels.bdp.circustrain.core.client.DataManipulationClientFactoryManager;
import com.hotels.bdp.circustrain.core.event.EventUtils;
import com.hotels.bdp.circustrain.core.replica.hive.AlterTableService;
import com.hotels.bdp.circustrain.core.replica.hive.DropTableService;
import com.hotels.hcommon.hive.metastore.client.api.CloseableMetaStoreClient;
import com.hotels.hcommon.hive.metastore.exception.MetaStoreClientException;
import com.hotels.hcommon.hive.metastore.util.LocationUtils;

public class Replica extends HiveEndpoint {

  private static final Logger LOG = LoggerFactory.getLogger(Replica.class);

  private final ReplicaTableFactory tableFactory;
  private final HousekeepingListener housekeepingListener;
  private final ReplicaCatalogListener replicaCatalogListener;
  private final ReplicationMode replicationMode;
  private final TableReplication tableReplication;
  private final AlterTableService alterTableService;
  private int partitionBatchSize = 1000;

  /**
   * Use {@link ReplicaFactory}
   */
  Replica(
      ReplicaCatalog replicaCatalog,
      HiveConf replicaHiveConf,
      Supplier<CloseableMetaStoreClient> replicaMetaStoreClientSupplier,
      ReplicaTableFactory replicaTableFactory,
      HousekeepingListener housekeepingListener,
      ReplicaCatalogListener replicaCatalogListener,
      TableReplication tableReplication,
      AlterTableService alterTableService) {
    super(replicaCatalog.getName(), replicaHiveConf, replicaMetaStoreClientSupplier);
    this.replicaCatalogListener = replicaCatalogListener;
    tableFactory = replicaTableFactory;
    this.housekeepingListener = housekeepingListener;
    replicationMode = tableReplication.getReplicationMode();
    this.tableReplication = tableReplication;
    this.alterTableService = alterTableService;
  }

  /**
   * Use {@link ReplicaFactory}
   */
  @VisibleForTesting
  Replica(
      ReplicaCatalog replicaCatalog,
      HiveConf replicaHiveConf,
      Supplier<CloseableMetaStoreClient> replicaMetaStoreClientSupplier,
      ReplicaTableFactory replicaTableFactory,
      HousekeepingListener housekeepingListener,
      ReplicaCatalogListener replicaCatalogListener,
      TableReplication tableReplication,
      AlterTableService alterTableService,
      int partitionBatchSize) {
    super(replicaCatalog.getName(), replicaHiveConf, replicaMetaStoreClientSupplier);
    this.replicaCatalogListener = replicaCatalogListener;
    tableFactory = replicaTableFactory;
    this.housekeepingListener = housekeepingListener;
    replicationMode = tableReplication.getReplicationMode();
    this.tableReplication = tableReplication;
    this.partitionBatchSize = partitionBatchSize;
    this.alterTableService = alterTableService;
  }

  public void updateMetadata(
      String eventId,
      TableAndStatistics sourceTable,
      String replicaDatabaseName,
      String replicaTableName,
      ReplicaLocationManager locationManager) {
    try (CloseableMetaStoreClient client = getMetaStoreClientSupplier().get()) {
      Optional<Table> oldReplicaTable = updateTableMetadata(client, eventId, sourceTable, replicaDatabaseName,
          replicaTableName, locationManager.getTableLocation(), replicationMode);
      if (oldReplicaTable.isPresent()
          && LocationUtils.hasLocation(oldReplicaTable.get())
          && isUnpartitioned(oldReplicaTable.get())) {

        Path oldLocation = locationAsPath(oldReplicaTable.get());
        String oldEventId = oldReplicaTable.get().getParameters().get(REPLICATION_EVENT.parameterName());
        locationManager.addCleanUpLocation(oldEventId, oldLocation);
      }
    }
  }

  private boolean isUnpartitioned(Table table) {
    return table.getPartitionKeysSize() == 0;
  }

  public void updateMetadata(
      String eventId,
      TableAndStatistics sourceTableAndStatistics,
      PartitionsAndStatistics sourcePartitionsAndStatistics,
      String replicaDatabaseName,
      String replicaTableName,
      ReplicaLocationManager locationManager) {
    try (CloseableMetaStoreClient client = getMetaStoreClientSupplier().get()) {

      updateTableMetadata(client, eventId, sourceTableAndStatistics, replicaDatabaseName, replicaTableName,
          locationManager.getTableLocation(), replicationMode);

      List<Partition> oldPartitions = getOldPartitions(sourcePartitionsAndStatistics, replicaDatabaseName,
          replicaTableName, client);
      LOG.debug("Found {} existing partitions that may match.", oldPartitions.size());

      replicaCatalogListener
          .existingReplicaPartitions(EventUtils.toEventPartitions(sourceTableAndStatistics.getTable(), oldPartitions));

      Map<List<String>, Partition> oldPartitionsByKey = mapPartitionsByKey(oldPartitions);

      List<Partition> sourcePartitions = sourcePartitionsAndStatistics.getPartitions();
      List<Partition> partitionsToCreate = new ArrayList<>(sourcePartitions.size());
      List<Partition> partitionsToAlter = new ArrayList<>(sourcePartitions.size());
      List<ColumnStatistics> statisticsToSet = new ArrayList<>(sourcePartitions.size());
      for (Partition sourcePartition : sourcePartitions) {
        Path replicaPartitionLocation = locationManager.getPartitionLocation(sourcePartition);
        LOG.debug("Generated replica partition path: {}", replicaPartitionLocation);

        Partition replicaPartition = tableFactory
            .newReplicaPartition(eventId, sourceTableAndStatistics.getTable(), sourcePartition, replicaDatabaseName,
                replicaTableName, replicaPartitionLocation, replicationMode);
        Partition oldPartition = oldPartitionsByKey.get(sourcePartition.getValues());
        if (oldPartition == null) {
          partitionsToCreate.add(replicaPartition);
        } else {
          partitionsToAlter.add(replicaPartition);
          if (LocationUtils.hasLocation(oldPartition)) {
            Path oldLocation = locationAsPath(oldPartition);
            String oldEventId = oldPartition.getParameters().get(REPLICATION_EVENT.parameterName());
            locationManager.addCleanUpLocation(oldEventId, oldLocation);
          }
        }

        ColumnStatistics sourcePartitionStatistics = sourcePartitionsAndStatistics
            .getStatisticsForPartition(sourcePartition);
        if (sourcePartitionStatistics != null) {
          statisticsToSet
              .add(tableFactory
                  .newReplicaPartitionStatistics(sourceTableAndStatistics.getTable(), replicaPartition,
                      sourcePartitionStatistics));
        }
      }
      replicaCatalogListener
          .partitionsToAlter(EventUtils.toEventPartitions(sourceTableAndStatistics.getTable(), partitionsToAlter));
      replicaCatalogListener
          .partitionsToCreate(EventUtils.toEventPartitions(sourceTableAndStatistics.getTable(), partitionsToCreate));

      if (!partitionsToCreate.isEmpty()) {
        LOG.info("Creating {} new partitions.", partitionsToCreate.size());
        try {
          int counter = 0;
          for (List<Partition> sublist : Lists.partition(partitionsToCreate, partitionBatchSize)) {
            int start = counter * partitionBatchSize;
            LOG.info("Creating partitions {} through {}", start, start + sublist.size() - 1);
            client.add_partitions(sublist);
            counter++;
          }
        } catch (TException e) {
          throw new MetaStoreClientException("Unable to add partitions '"
              + partitionsToCreate
              + "' to replica table '"
              + replicaDatabaseName
              + "."
              + replicaTableName
              + "'", e);
        }
      }
      if (!partitionsToAlter.isEmpty()) {
        LOG.info("Altering {} existing partitions.", partitionsToAlter.size());
        try {
          int counter = 0;
          for (List<Partition> sublist : Lists.partition(partitionsToAlter, partitionBatchSize)) {
            int start = counter * partitionBatchSize;
            LOG.info("Altering partitions {} through {}", start, start + sublist.size() - 1);
            client.alter_partitions(replicaDatabaseName, replicaTableName, sublist);
            counter++;
          }
        } catch (TException e) {
          throw new MetaStoreClientException("Unable to alter partitions '"
              + partitionsToAlter
              + "' of replica table '"
              + replicaDatabaseName
              + "."
              + replicaTableName
              + "'", e);
        }
      }
      if (!statisticsToSet.isEmpty()) {
        LOG.info("Setting column statistics for {} partitions.", statisticsToSet.size());
        try {
          int counter = 0;
          for (List<ColumnStatistics> sublist : Lists.partition(statisticsToSet, partitionBatchSize)) {
            int start = counter * partitionBatchSize;
            LOG.info("Setting column statistics for partitions {} through {}", start, start + sublist.size() - 1);
            client.setPartitionColumnStatistics(new SetPartitionsStatsRequest(sublist));
            counter++;
          }
        } catch (TException e) {
          throw new MetaStoreClientException(
              "Unable to set column statistics of replica table '" + replicaDatabaseName + "." + replicaTableName + "'",
              e);
        }
      } else {
        LOG.debug("No partition column stats to set.");
      }
    }
  }

  private List<Partition> getOldPartitions(
      PartitionsAndStatistics sourcePartitionsAndStatistics,
      String replicaDatabaseName,
      String replicaTableName,
      CloseableMetaStoreClient client) {
    List<String> partitionValues = sourcePartitionsAndStatistics.getPartitionNames();
    try {
      return client.getPartitionsByNames(replicaDatabaseName, replicaTableName, partitionValues);
    } catch (TException e) {
      throw new MetaStoreClientException("Unable to list current partitions of replica table '"
          + replicaDatabaseName
          + "."
          + replicaTableName
          + "' with partition values '"
          + partitionValues
          + "'", e);
    }
  }

  private Map<List<String>, Partition> mapPartitionsByKey(List<Partition> partitions) {
    Map<List<String>, Partition> partitionsByKey = new HashMap<>(partitions.size());
    for (Partition partition : partitions) {
      partitionsByKey.put(partition.getValues(), partition);
    }
    return partitionsByKey;
  }

  private Optional<Table> updateTableMetadata(
      CloseableMetaStoreClient client,
      String eventId,
      TableAndStatistics sourceTable,
      String replicaDatabaseName,
      String replicaTableName,
      Path tableLocation,
      ReplicationMode replicationMode) {
    LOG.info("Updating replica table metadata.");
    TableAndStatistics replicaTable = tableFactory
        .newReplicaTable(eventId, sourceTable, replicaDatabaseName, replicaTableName, tableLocation, replicationMode);

    Optional<Table> oldReplicaTable = getTable(client, replicaDatabaseName, replicaTableName);
    if (!oldReplicaTable.isPresent()) {
      LOG.debug("No existing replica table found, creating.");
      try {
        client.createTable(replicaTable.getTable());
        updateTableColumnStatistics(client, replicaTable);
      } catch (TException e) {
        throw new MetaStoreClientException(
            "Unable to create replica table '" + replicaDatabaseName + "." + replicaTableName + "'", e);
      }
    } else {
      makeSureCanReplicate(oldReplicaTable.get(), replicaTable.getTable());
      LOG.debug("Existing replica table found, altering.");
      try {
        alterTableService.alterTable(client, oldReplicaTable.get(), replicaTable.getTable());
        updateTableColumnStatistics(client, replicaTable);
      } catch (TException e) {
        throw new MetaStoreClientException(
            "Unable to alter replica table '" + replicaDatabaseName + "." + replicaTableName + "'", e);
      }
    }
    return oldReplicaTable;
  }

  private void makeSureCanReplicate(Table oldReplicaTable, Table replicaTable) {
    if (!Objects.equals(oldReplicaTable.getTableType(), replicaTable.getTableType())) {
      String message = String
          .format("Unable to replace %s %s.%s with %s %s.%s", oldReplicaTable.getTableType(),
              oldReplicaTable.getDbName(), oldReplicaTable.getTableName(), replicaTable.getTableType(),
              replicaTable.getDbName(), replicaTable.getTableName());
      throw new CircusTrainException(message);
    }
  }

  /**
   * Checks if there is a replica table and validates the replication modes.
   *
   * @throws CircusTrainException if the replica is invalid and the table can't be replicated.
   */
  public void validateReplicaTable(String replicaDatabaseName, String replicaTableName) {
    try (CloseableMetaStoreClient client = getMetaStoreClientSupplier().get()) {
      Optional<Table> oldReplicaTable = getTable(client, replicaDatabaseName, replicaTableName);
      if (oldReplicaTable.isPresent()) {
        LOG.debug("Existing table found, checking that it is a valid replica.");
        determineValidityOfReplica(replicationMode, oldReplicaTable.get());
      }
    }
  }

  private void determineValidityOfReplica(ReplicationMode replicationMode, Table oldReplicaTable) {
    // REPLICATION_MODE is a table parameter that was added later it might not be set, so we're checking the
    // REPLICATION_EVENT to determine if a table was created via CT.
    String previousEvent = oldReplicaTable.getParameters().get(REPLICATION_EVENT.parameterName());
    if (StringUtils.isBlank(previousEvent)) {
      throw new DestinationNotReplicaException(oldReplicaTable, getHiveConf().getVar(ConfVars.METASTOREURIS),
          REPLICATION_EVENT);
    }
    LOG.debug("Checking that replication modes are compatible.");
    Optional<ReplicationMode> replicaReplicationMode = Enums
        .getIfPresent(ReplicationMode.class,
            nullToEmpty(oldReplicaTable.getParameters().get(REPLICATION_MODE.parameterName())));
    if (replicaReplicationMode.isPresent()) {
      if (replicaReplicationMode.get() == METADATA_MIRROR && replicationMode != METADATA_MIRROR) {
        throw new InvalidReplicationModeException("Trying a "
            + replicationMode.name()
            + " replication on a table that was previously only "
            + METADATA_MIRROR.name()
            + "-ed. This is not possible, rerun with a different table name or change the replication mode to "
            + METADATA_MIRROR.name()
            + ".");
      }
      if (replicaReplicationMode.get() != METADATA_MIRROR && replicationMode == METADATA_MIRROR) {
        throw new InvalidReplicationModeException("Trying to "
            + METADATA_MIRROR.name()
            + " a previously replicated table. This is not possible, rerun with a different table name or"
            + " change the replication mode to "
            + FULL.name()
            + ", "
            + FULL_OVERWRITE.name()
            + ", or "
            + METADATA_UPDATE.name()
            + ".");
      }
    } else if (replicationMode == METADATA_MIRROR) {
      // no replicaReplicationMode found in table settings we assume FULL_REPLICATION was intended.
      throw new InvalidReplicationModeException("Trying to "
          + METADATA_MIRROR.name()
          + " a previously replicated table. This is not possible, rerun with a different table name or"
          + " change the replication mode to "
          + FULL.name()
          + ", "
          + FULL_OVERWRITE.name()
          + ", or "
          + METADATA_UPDATE.name()
          + ".");
    }
    LOG.debug("Replication modes are compatible.");
  }

  private void updateTableColumnStatistics(CloseableMetaStoreClient client, TableAndStatistics replicaTable)
    throws TException {
    if (replicaTable.getStatistics() != null) {
      LOG
          .debug("Updating {} column statistics for table {}.{}", replicaTable.getStatistics().getStatsObj().size(),
              replicaTable.getTable().getDbName(), replicaTable.getTable().getTableName());
      client.updateTableColumnStatistics(replicaTable.getStatistics());
    } else {
      LOG
          .debug("No column statistics to update for table {}.{}", replicaTable.getTable().getDbName(),
              replicaTable.getTable().getTableName());
    }
  }

  public ReplicaLocationManager getLocationManager(
      TableType tableType,
      String targetTableLocation,
      String eventId,
      SourceLocationManager sourceLocationManager) {
    CleanupLocationManager cleanupLocationManager = CleanupLocationManagerFactory
        .newInstance(eventId, housekeepingListener, replicaCatalogListener, tableReplication);
    return new FullReplicationReplicaLocationManager(sourceLocationManager, targetTableLocation, eventId, tableType,
        cleanupLocationManager, replicaCatalogListener);
  }

  @Override
  public TableAndStatistics getTableAndStatistics(TableReplication tableReplication) {
    return super.getTableAndStatistics(tableReplication.getReplicaDatabaseName(),
        tableReplication.getReplicaTableName());
  }

  public void checkIfReplicaCleanupRequired(
      String replicaDatabaseName,
      String replicaTableName,
      DataManipulationClientFactoryManager dataManipulationClientFactoryManager) {

    try (CloseableMetaStoreClient client = getMetaStoreClientSupplier().get()) {
      if (replicationMode == FULL_OVERWRITE) {
        LOG.debug("Replication mode: FULL_OVERWRITE. Checking for existing replica table.");
        DropTableService dropTableService = new DropTableService();
        try {
          // DataManipulationClientFactory dataManipulationClientFactory = dataManipulationClientFactoryManager.get
          dropTableService
              .dropTableAndData(client, replicaDatabaseName, replicaTableName, dataManipulationClientFactoryManager);
        } catch (Exception e) {
          LOG.info("Replica table '" + replicaDatabaseName + "." + replicaTableName + "' was not dropped.");
        }
      }
    }

  }

}
