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
package com.hotels.bdp.circustrain.core.replica;

import static com.hotels.bdp.circustrain.api.CircusTrainTableParameter.LAST_REPLICATED;
import static com.hotels.bdp.circustrain.api.CircusTrainTableParameter.REPLICATION_EVENT;
import static com.hotels.bdp.circustrain.api.CircusTrainTableParameter.REPLICATION_MODE;
import static com.hotels.bdp.circustrain.api.CircusTrainTableParameter.SOURCE_LOCATION;
import static com.hotels.bdp.circustrain.api.CircusTrainTableParameter.SOURCE_METASTORE;
import static com.hotels.bdp.circustrain.api.CircusTrainTableParameter.SOURCE_TABLE;

import java.util.Map;
import java.util.Objects;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsDesc;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import com.hotels.bdp.circustrain.api.conf.OrphanedDataStrategy;
import com.hotels.bdp.circustrain.api.conf.ReplicationMode;
import com.hotels.bdp.circustrain.api.metadata.ColumnStatisticsTransformation;
import com.hotels.bdp.circustrain.api.metadata.PartitionTransformation;
import com.hotels.bdp.circustrain.api.metadata.TableTransformation;
import com.hotels.bdp.circustrain.core.TableAndStatistics;
import com.hotels.bdp.circustrain.core.annotation.TableAnnotator;
import com.hotels.bdp.circustrain.core.annotation.TableAnnotatorFactory;

public class ReplicaTableFactory {

  /**
   * Parameter to indicate we wish to retain table statistics >= Hive 2.0.0.
   */
  private static final String DO_NOT_UPDATE_STATS = StatsSetupConst.DO_NOT_UPDATE_STATS;

  /**
   * Parameter to indicate we wish to retain table statistics <= Hive 2.0.1.
   */
  private static final String STATS_GENERATED_VIA_STATS_TASK = "STATS_GENERATED_VIA_STATS_TASK";

  /**
   * Parameter to indicate we wish to retain table statistics >= Hive 2.1.0.
   */
  private static final String STATS_GENERATED = StatsSetupConst.STATS_GENERATED;

  /**
   * Parameter to indicate the "EXTERNAL" table type. Values "TRUE" or "FALSE".
   */
  private static final String EXTERNAL = "EXTERNAL";

  private final String sourceMetaStoreUris;
  private final TableTransformation tableTransformation;
  private final PartitionTransformation partitionTransformation;
  private final ColumnStatisticsTransformation columnStatisticsTransformation;

  ReplicaTableFactory(
      HiveConf sourceHiveConf,
      TableTransformation tableTransformation,
      PartitionTransformation partitionTransformation,
      ColumnStatisticsTransformation columnStatisticsTransformation) {
    this(sourceHiveConf.getVar(ConfVars.METASTOREURIS), tableTransformation, partitionTransformation,
        columnStatisticsTransformation);
  }

  ReplicaTableFactory(
      String sourceMetaStoreUris,
      TableTransformation tableTransformation,
      PartitionTransformation partitionTransformation,
      ColumnStatisticsTransformation columnStatisticsTransformation) {
    this.sourceMetaStoreUris = sourceMetaStoreUris;
    this.tableTransformation = tableTransformation;
    this.partitionTransformation = partitionTransformation;
    this.columnStatisticsTransformation = columnStatisticsTransformation;
  }

  TableAndStatistics newReplicaTable(
      String eventId,
      TableAndStatistics sourceTableAndStatistics,
      String replicaDatabaseName,
      String replicaTableName,
      Path replicaDataDestination,
      ReplicationMode replicationMode,
      OrphanedDataStrategy orphanedDataStrategy,
      Map<String, String> orphanedDataOptions) {
    Table sourceTable = sourceTableAndStatistics.getTable();
    Table replica = tableTransformation.transform(new Table(sourceTable));
    replica.setDbName(replicaDatabaseName);
    replica.setTableName(replicaTableName);
    replica.getSd().setLocation(toStringOrNull(replicaDataDestination));

    setReplicaTableType(sourceTable, replica);

    // Statistic specific parameters
    replica.putToParameters(STATS_GENERATED_VIA_STATS_TASK, Boolean.TRUE.toString());
    replica.putToParameters(STATS_GENERATED, Boolean.TRUE.toString());
    replica.putToParameters(DO_NOT_UPDATE_STATS, Boolean.TRUE.toString());

    // Replication specific parameters
    replica.putToParameters(LAST_REPLICATED.parameterName(), DateTime.now(DateTimeZone.UTC).toString());
    replica.putToParameters(REPLICATION_EVENT.parameterName(), eventId);
    replica.putToParameters(SOURCE_LOCATION.parameterName(), toStringOrEmpty(sourceTable.getSd().getLocation()));
    replica.putToParameters(SOURCE_TABLE.parameterName(), Warehouse.getQualifiedName(sourceTable));
    replica.putToParameters(SOURCE_METASTORE.parameterName(), sourceMetaStoreUris);
    replica.putToParameters(REPLICATION_MODE.parameterName(), replicationMode.name());

    TableAnnotator tableAnnotator = TableAnnotatorFactory.newInstance(replicationMode, orphanedDataStrategy);
    tableAnnotator.annotateTable(replica, orphanedDataOptions);

    // Create some replica stats
    ColumnStatistics replicaColumnStats = null;
    if (sourceTableAndStatistics.getStatistics() != null) {
      replicaColumnStats = new ColumnStatistics(
          new ColumnStatisticsDesc(true, replica.getDbName(), replica.getTableName()),
          sourceTableAndStatistics.getStatistics().getStatsObj());
      replicaColumnStats = columnStatisticsTransformation.transform(replicaColumnStats);
    }

    return new TableAndStatistics(replica, replicaColumnStats);
  }

  private String toStringOrEmpty(Object value) {
    return Objects.toString(value, "");
  }

  private String toStringOrNull(Object value) {
    return value != null ? value.toString() : null;
  }

  private void setReplicaTableType(Table source, Table replica) {
    if (TableType.VIRTUAL_VIEW.name().equals(source.getTableType())) {
      replica.setTableType(TableType.VIRTUAL_VIEW.name());
      return;
    }
    // We set the table to external no matter what. We don't want to delete data accidentally when dropping a mirrored
    // table.
    replica.setTableType(TableType.EXTERNAL_TABLE.name());
    replica.putToParameters(EXTERNAL, "TRUE");
  }

  Partition newReplicaPartition(
      String eventId,
      Table sourceTable,
      Partition sourcePartition,
      String replicaDatabaseName,
      String replicaTableName,
      Path replicaPartitionLocation,
      ReplicationMode replicationMode) {
    Partition replica = partitionTransformation.transform(new Partition(sourcePartition));
    replica.setDbName(replicaDatabaseName);
    replica.setTableName(replicaTableName);
    if (replica.getSd() != null) {
      replica.getSd().setLocation(toStringOrNull(replicaPartitionLocation));
    }

    String sourcePartitionLocation = sourcePartition.getSd() == null ? ""
        : toStringOrEmpty(sourcePartition.getSd().getLocation());

    // Statistic specific parameters
    replica.putToParameters(STATS_GENERATED_VIA_STATS_TASK, Boolean.TRUE.toString());
    replica.putToParameters(STATS_GENERATED, Boolean.TRUE.toString());
    replica.putToParameters(DO_NOT_UPDATE_STATS, Boolean.TRUE.toString());
    // Replication specific parameters
    replica.putToParameters(LAST_REPLICATED.parameterName(), DateTime.now(DateTimeZone.UTC).toString());
    replica.putToParameters(REPLICATION_EVENT.parameterName(), eventId);
    replica.putToParameters(SOURCE_LOCATION.parameterName(), sourcePartitionLocation);
    replica.putToParameters(SOURCE_TABLE.parameterName(), Warehouse.getQualifiedName(sourceTable));
    replica.putToParameters(SOURCE_METASTORE.parameterName(), sourceMetaStoreUris);
    replica.putToParameters(REPLICATION_MODE.parameterName(), replicationMode.name());
    return replica;
  }

  ColumnStatistics newReplicaPartitionStatistics(
      Table replicaTable,
      Partition replicaPartition,
      ColumnStatistics sourcePartitionStatistics) {
    ColumnStatisticsDesc statisticsDesc = new ColumnStatisticsDesc(false, replicaPartition.getDbName(),
        replicaPartition.getTableName());
    try {
      statisticsDesc.setPartName(Warehouse.makePartName(replicaTable.getPartitionKeys(), replicaPartition.getValues()));
    } catch (MetaException e) {
      throw new RuntimeException(e);
    }

    return columnStatisticsTransformation
        .transform(new ColumnStatistics(statisticsDesc, sourcePartitionStatistics.getStatsObj()));
  }

}
