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

package com.hotels.bdp.circustrain.core;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsDesc;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.google.common.base.Optional;
import com.google.common.base.Strings;
import com.google.common.base.Supplier;

import com.hotels.bdp.circustrain.api.CircusTrainException;
import com.hotels.bdp.circustrain.core.conf.TableReplication;
import com.hotels.hcommon.hive.metastore.exception.MetaStoreClientException;
import com.hotels.hcommon.hive.metastore.client.api.CloseableMetaStoreClient;

public abstract class HiveEndpoint {

  private final Logger log = LoggerFactory.getLogger(getClass());

  private final String name;
  private final HiveConf hiveConf;
  private final Supplier<CloseableMetaStoreClient> metaStoreClientSupplier;

  @Autowired
  public HiveEndpoint(String name, HiveConf hiveConf, Supplier<CloseableMetaStoreClient> metaStoreClientSupplier) {
    this.name = name;
    this.hiveConf = hiveConf;
    this.metaStoreClientSupplier = metaStoreClientSupplier;
  }

  public String getName() {
    return name;
  }

  public HiveConf getHiveConf() {
    return hiveConf;
  }

  public String getMetaStoreUris() {
    return hiveConf.getVar(ConfVars.METASTOREURIS);
  }

  public Supplier<CloseableMetaStoreClient> getMetaStoreClientSupplier() {
    return metaStoreClientSupplier;
  }

  public Database getDatabase(String database) {
    log.debug("Retrieving database metadata for '{}'", database);
    try (CloseableMetaStoreClient client = metaStoreClientSupplier.get()) {
      return client.getDatabase(database);
    } catch (NoSuchObjectException e) {
      String message = String.format("Database '%s' not found", database);
      throw new CircusTrainException(message, e);
    } catch (Exception e) {
      String message = String.format("Cannot fetch database metadata for '%s'", database);
      throw new MetaStoreClientException(message, e);
    }
  }

  /**
   * @return Hive table from the metastore if not found returns {@link Optional#absent()}
   */
  public Optional<Table> getTable(CloseableMetaStoreClient client, String database, String table) {
    Table oldReplicaTable = null;
    try {
      log.debug("Checking for existing table {}.{}", database, table);
      oldReplicaTable = client.getTable(database, table);
      log.debug("Existing table found.");
    } catch (NoSuchObjectException e) {
      log.debug("Table '{}.{}' not found.", database, table);
    } catch (TException e) {
      String message = String.format("Cannot fetch table metadata for '%s.%s'", database, table);
      log.error(message, e);
      throw new MetaStoreClientException(message, e);
    }
    return Optional.fromNullable(oldReplicaTable);
  }

  public TableAndStatistics getTableAndStatistics(String database, String tableName) {
    log.info("Retrieving table metadata for '{}.{}'", database, tableName);
    try (CloseableMetaStoreClient client = metaStoreClientSupplier.get()) {
      Table table = client.getTable(database, tableName);
      List<String> columnNames = getColumnNames(table);
      List<ColumnStatisticsObj> statisticsObj = client.getTableColumnStatistics(database, tableName, columnNames);
      ColumnStatistics statistics = null;
      if (statisticsObj != null && !statisticsObj.isEmpty()) {
        statistics = new ColumnStatistics(new ColumnStatisticsDesc(true, table.getDbName(), table.getTableName()),
            statisticsObj);
        log.debug("Retrieved {} column stats entries for table {}.{}", statisticsObj.size(), table.getDbName(),
            table.getTableName());
      } else {
        log.debug("No table column stats retrieved for table {}.{}", table.getDbName(), table.getTableName());
      }
      return new TableAndStatistics(table, statistics);
    } catch (NoSuchObjectException e) {
      String message = String.format("Table '%s.%s' not found", database, tableName);
      throw new CircusTrainException(message, e);
    } catch (TException e) {
      String message = String.format("Cannot fetch table metadata for '%s.%s'", database, tableName);
      throw new MetaStoreClientException(message, e);
    }
  }

  abstract public TableAndStatistics getTableAndStatistics(TableReplication tableReplication);

  private List<String> getColumnNames(Table table) {
    List<FieldSchema> fields = table.getSd().getCols();
    List<String> columnNames = new ArrayList<>(fields.size());
    for (FieldSchema field : fields) {
      columnNames.add(field.getName());
    }
    return columnNames;
  }

  public PartitionsAndStatistics getPartitions(Table table, String partitionPredicate, int maxPartitions)
      throws TException {
    try (CloseableMetaStoreClient client = metaStoreClientSupplier.get()) {
      List<Partition> partitions = null;
      if (Strings.isNullOrEmpty(partitionPredicate)) {
        partitions = client.listPartitions(table.getDbName(), table.getTableName(), (short) maxPartitions);
      } else {
        partitions = client.listPartitionsByFilter(table.getDbName(), table.getTableName(), partitionPredicate,
            (short) maxPartitions);
      }

      // Generate a list of partition names
      List<String> partitionNames = getPartitionNames(table.getPartitionKeys(), partitions);
      // Fetch the partition statistics
      List<String> columnNames = getColumnNames(table);

      Map<String, List<ColumnStatisticsObj>> statisticsByPartitionName = client
          .getPartitionColumnStatistics(table.getDbName(), table.getTableName(), partitionNames, columnNames);
      if (statisticsByPartitionName != null && !statisticsByPartitionName.isEmpty()) {
        log.debug("Retrieved column stats entries for {} partitions of table {}.{}", statisticsByPartitionName.size(),
            table.getDbName(), table.getTableName());
      } else {
        log.debug("No partition column stats retrieved for table {}.{}", table.getDbName(), table.getTableName());
      }

      return new PartitionsAndStatistics(table.getPartitionKeys(), partitions, statisticsByPartitionName);
    }
  }

  private List<String> getPartitionNames(List<FieldSchema> partitionKeys, List<Partition> partitions)
      throws MetaException {
    List<String> partitionNames = new ArrayList<>(partitions.size());
    for (Partition partition : partitions) {
      partitionNames.add(Warehouse.makePartName(partitionKeys, partition.getValues()));
    }
    return partitionNames;
  }

}
