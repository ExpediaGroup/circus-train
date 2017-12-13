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
package com.hotels.bdp.circustrain.core.replica;

import static com.google.common.base.Strings.nullToEmpty;

import static com.hotels.bdp.circustrain.api.CircusTrainTableParameter.REPLICATION_EVENT;
import static com.hotels.bdp.circustrain.api.CircusTrainTableParameter.REPLICATION_MODE;
import static com.hotels.bdp.circustrain.core.conf.ReplicationMode.FULL;
import static com.hotels.bdp.circustrain.core.conf.ReplicationMode.METADATA_MIRROR;
import static com.hotels.bdp.circustrain.core.conf.ReplicationMode.METADATA_UPDATE;
import static com.hotels.bdp.circustrain.core.metastore.LocationUtils.locationAsPath;

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
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SetPartitionsStatsRequest;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Enums;
import com.google.common.base.Optional;
import com.google.common.base.Supplier;

import com.hotels.bdp.circustrain.api.CircusTrainException;
import com.hotels.bdp.circustrain.api.ReplicaLocationManager;
import com.hotels.bdp.circustrain.api.SourceLocationManager;
import com.hotels.bdp.circustrain.api.event.HousekeepingListener;
import com.hotels.bdp.circustrain.api.event.ReplicaCatalogListener;
import com.hotels.bdp.circustrain.api.metastore.CloseableMetaStoreClient;
import com.hotels.bdp.circustrain.api.metastore.MetaStoreClientException;
import com.hotels.bdp.circustrain.core.HiveEndpoint;
import com.hotels.bdp.circustrain.core.PartitionsAndStatistics;
import com.hotels.bdp.circustrain.core.TableAndStatistics;
import com.hotels.bdp.circustrain.core.conf.ReplicaCatalog;
import com.hotels.bdp.circustrain.core.conf.ReplicationMode;
import com.hotels.bdp.circustrain.core.conf.TableReplication;
import com.hotels.bdp.circustrain.core.event.EventUtils;
import com.hotels.bdp.circustrain.core.metastore.LocationUtils;

public class Replica extends HiveEndpoint {

  private static final Logger LOG = LoggerFactory.getLogger(Replica.class);

  private final ReplicaTableFactory tableFactory;
  private final HousekeepingListener housekeepingListener;
  private final ReplicaCatalogListener replicaCatalogListener;
  private final ReplicationMode replicationMode;
  private final EnvironmentContext environmentContext;

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
      ReplicationMode replicationMode) {
    super(replicaCatalog.getName(), replicaHiveConf, replicaMetaStoreClientSupplier);
    this.replicaCatalogListener = replicaCatalogListener;
    tableFactory = replicaTableFactory;
    this.housekeepingListener = housekeepingListener;
    this.replicationMode = replicationMode;
    environmentContext = new EnvironmentContext(); // TODO if we have to pass properties, which ones should we pass?
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
      if (oldReplicaTable.isPresent() && LocationUtils.hasLocation(oldReplicaTable.get())) {
        Path oldLocation = locationAsPath(oldReplicaTable.get());
        String oldEventId = oldReplicaTable.get().getParameters().get(REPLICATION_EVENT.parameterName());
        locationManager.addCleanUpLocation(oldEventId, oldLocation);
      }
    }
  }

  public void updateMetadata(
      String eventId,
      TableAndStatistics sourceTableAndStatistics,
      PartitionsAndStatistics sourcePartitionsAndStatistics,
      SourceLocationManager sourceLocationManager,
      String replicaDatabaseName,
      String replicaTableName,
      ReplicaLocationManager locationManager) {
    try (CloseableMetaStoreClient client = getMetaStoreClientSupplier().get()) {
      updateTableMetadata(client, eventId, sourceTableAndStatistics, replicaDatabaseName, replicaTableName,
          locationManager.getTableLocation(), replicationMode);

      List<Partition> oldPartitions = getOldPartitions(sourceTableAndStatistics, sourcePartitionsAndStatistics,
          replicaDatabaseName, replicaTableName, client);
      LOG.debug("Found {} existing partitions that may match.", oldPartitions.size());

      replicaCatalogListener.existingReplicaPartitions(EventUtils.toEventPartitions(oldPartitions));

      Map<List<String>, Partition> oldPartitionsByKey = mapPartitionsByKey(oldPartitions);

      List<Partition> sourcePartitions = sourcePartitionsAndStatistics.getPartitions();
      List<Partition> partitionsToCreate = new ArrayList<>(sourcePartitions.size());
      List<Partition> partitionsToAlter = new ArrayList<>(sourcePartitions.size());
      List<ColumnStatistics> statisticsToSet = new ArrayList<>(sourcePartitions.size());
      for (Partition sourcePartition : sourcePartitions) {
        Path replicaPartitionLocation = locationManager.getPartitionLocation(sourcePartition);
        LOG.debug("Generated replica partition path: {}", replicaPartitionLocation);

        Partition replicaPartition = tableFactory.newReplicaPartition(eventId, sourceTableAndStatistics.getTable(),
            sourcePartition, replicaDatabaseName, replicaTableName, replicaPartitionLocation, replicationMode);

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
          statisticsToSet.add(tableFactory.newReplicaPartitionStatistics(sourceTableAndStatistics.getTable(),
              replicaPartition, sourcePartitionStatistics));
        }
      }

      replicaCatalogListener.partitionsToAlter(EventUtils.toEventPartitions(partitionsToAlter));
      replicaCatalogListener.partitionsToCreate(EventUtils.toEventPartitions(partitionsToCreate));

      if (!partitionsToCreate.isEmpty()) {
        LOG.info("Creating {} new partitions.", partitionsToCreate.size());
        try {
          client.add_partitions(partitionsToCreate);
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
          client.alter_partitions(replicaDatabaseName, replicaTableName, partitionsToAlter, environmentContext);
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
          client.setPartitionColumnStatistics(new SetPartitionsStatsRequest(statisticsToSet));
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
      TableAndStatistics sourceTableAndStatistics,
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
    TableAndStatistics replicaTable = tableFactory.newReplicaTable(eventId, sourceTable, replicaDatabaseName,
        replicaTableName, tableLocation, replicationMode);
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
        client.alter_table(replicaDatabaseName, replicaTableName, replicaTable.getTable());
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
      String message = String.format("Unable to replace %s %s.%s with %s %s.%s", oldReplicaTable.getTableType(),
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
        determinValidityOfReplica(replicationMode, oldReplicaTable.get());
      }
    }
  }

  private void determinValidityOfReplica(ReplicationMode replicationMode, Table oldReplicaTable) {
    // REPLICATION_MODE is a table parameter that was added later it might not be set, so we're checking the
    // REPLICATION_EVENT to determine if a table was created via CT.
    String previousEvent = oldReplicaTable.getParameters().get(REPLICATION_EVENT.parameterName());
    if (StringUtils.isBlank(previousEvent)) {
      throw new DestinationNotReplicaException(oldReplicaTable, getHiveConf().getVar(ConfVars.METASTOREURIS));
    }
    LOG.debug("Checking that replication modes are compatible.");
    Optional<ReplicationMode> replicaReplicationMode = Enums.getIfPresent(ReplicationMode.class,
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
            + " a previously replicated table. This is not possible, rerun with a different table name or change the replication mode to "
            + FULL.name()
            + " or "
            + METADATA_UPDATE.name()
            + ".");
      }
    } else if (replicationMode == METADATA_MIRROR) {
      // no replicaReplicationMode found in table settings we assume FULL_REPLICATION was intended.
      throw new InvalidReplicationModeException("Trying to "
          + METADATA_MIRROR.name()
          + " a previously replicated table. This is not possible, rerun with a different table name or change the replication mode to "
          + FULL.name()
          + " or "
          + METADATA_UPDATE.name()
          + ".");
    }
  }

  private void updateTableColumnStatistics(CloseableMetaStoreClient client, TableAndStatistics replicaTable)
    throws TException {
    if (replicaTable.getStatistics() != null) {
      LOG.debug("Updating {} column statistics for table {}.{}", replicaTable.getStatistics().getStatsObj().size(),
          replicaTable.getTable().getDbName(), replicaTable.getTable().getTableName());
      client.updateTableColumnStatistics(replicaTable.getStatistics());
    } else {
      LOG.debug("No column statistics to update for table {}.{}", replicaTable.getTable().getDbName(),
          replicaTable.getTable().getTableName());
    }
  }

  public ReplicaLocationManager getLocationManager(
      TableType tableType,
      String targetTableLocation,
      String eventId,
      SourceLocationManager sourceLocationManager)
    throws TException {
    return new FullReplicationReplicaLocationManager(sourceLocationManager, targetTableLocation, eventId, tableType,
        housekeepingListener, replicaCatalogListener);
  }

  @Override
  public TableAndStatistics getTableAndStatistics(TableReplication tableReplication) {
    return super.getTableAndStatistics(tableReplication.getReplicaDatabaseName(),
        tableReplication.getReplicaTableName());
  }
}
