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

import static com.hotels.bdp.circustrain.api.CircusTrainTableParameter.REPLICATION_EVENT;
import static com.hotels.hcommon.hive.metastore.util.LocationUtils.locationAsPath;

import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.base.Supplier;

import com.hotels.bdp.circustrain.api.CircusTrainTableParameter;
import com.hotels.bdp.circustrain.api.conf.TableReplication;
import com.hotels.hcommon.hive.metastore.client.api.CloseableMetaStoreClient;
import com.hotels.hcommon.hive.metastore.iterator.PartitionIterator;
import com.hotels.hcommon.hive.metastore.util.LocationUtils;

public class DestructiveReplica {

  private static final boolean DELETE_DATA = false;
  private final Supplier<CloseableMetaStoreClient> replicaMetaStoreClientSupplier;
  private final TableReplication tableReplication;
  private final String databaseName;
  private final String tableName;
  private final CleanupLocationManager cleanupLocationManager;

  public DestructiveReplica(
      Supplier<CloseableMetaStoreClient> replicaMetaStoreClientSupplier,
      CleanupLocationManager cleanupLocationManager,
      TableReplication tableReplication) {
    this.replicaMetaStoreClientSupplier = replicaMetaStoreClientSupplier;
    this.cleanupLocationManager = cleanupLocationManager;
    this.tableReplication = tableReplication;
    databaseName = tableReplication.getReplicaDatabaseName();
    tableName = tableReplication.getReplicaTableName();
  }

  public boolean tableExists() throws TException {
    try (CloseableMetaStoreClient client = replicaMetaStoreClientSupplier.get()) {
      return client.tableExists(databaseName, tableName);
    }
  }

  public boolean tableIsUnderCircusTrainControl() throws TException {
    try (CloseableMetaStoreClient client = replicaMetaStoreClientSupplier.get()) {
      String sourceTableParameterValue = client
          .getTable(databaseName, tableName)
          .getParameters()
          .get(CircusTrainTableParameter.SOURCE_TABLE.parameterName());
      if (sourceTableParameterValue != null) {
        String qualifiedName = tableReplication.getSourceTable().getQualifiedName();
        return qualifiedName.equals(sourceTableParameterValue);
      }
    }
    return false;
  }

  public void dropDeletedPartitions(final List<String> sourcePartitionNames) throws TException {
    try (CloseableMetaStoreClient client = replicaMetaStoreClientSupplier.get()) {

      dropAndDeletePartitions(client, new Predicate<String>() {
        @Override
        public boolean apply(String partitionName) {
          return sourcePartitionNames.contains(partitionName);
        }
      });
    }
  }

  private void dropAndDeletePartitions(CloseableMetaStoreClient client, Predicate<String> shouldDelete)
    throws TException {
    try {
      Table replicaTable = client.getTable(databaseName, tableName);
      List<FieldSchema> partitionKeys = replicaTable.getPartitionKeys();
      PartitionIterator partitionIterator = new PartitionIterator(client, replicaTable, (short) 1000);
      while (partitionIterator.hasNext()) {
        Partition replicaPartition = partitionIterator.next();
        List<String> values = replicaPartition.getValues();
        String partitionName = Warehouse.makePartName(partitionKeys, values);
        if (shouldDelete.apply(partitionName)) {
          client.dropPartition(databaseName, tableName, partitionName, DELETE_DATA);
          if (LocationUtils.hasLocation(replicaPartition)) {
            Path oldLocation = locationAsPath(replicaPartition);
            String oldEventId = replicaPartition.getParameters().get(REPLICATION_EVENT.parameterName());
            cleanupLocationManager.addCleanUpLocation(oldEventId, oldLocation);
          }
        }
      }
    } finally {
      cleanupLocationManager.cleanUpLocations();
    }
  }

  public void dropTable() throws TException {
    try {
      try (CloseableMetaStoreClient client = replicaMetaStoreClientSupplier.get()) {
        dropAndDeletePartitions(client, Predicates.<String> alwaysTrue());
        Table table = client.getTable(databaseName, tableName);
        client.dropTable(databaseName, tableName);
        if (LocationUtils.hasLocation(table)) {
          Path oldLocation = locationAsPath(table);
          String oldEventId = table.getParameters().get(REPLICATION_EVENT.parameterName());
          cleanupLocationManager.addCleanUpLocation(oldEventId, oldLocation);
        }
      }
    } finally {
      cleanupLocationManager.cleanUpLocations();
    }
  }

  public String getQualifiedTableName() {
    return tableReplication.getQualifiedReplicaName();
  }

}
