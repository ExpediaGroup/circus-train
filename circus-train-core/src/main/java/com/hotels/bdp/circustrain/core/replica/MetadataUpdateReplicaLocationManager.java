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

import static com.hotels.bdp.circustrain.core.replica.TableType.UNPARTITIONED;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.thrift.TException;

import com.hotels.bdp.circustrain.api.CircusTrainException;
import com.hotels.bdp.circustrain.api.ReplicaLocationManager;
import com.hotels.hcommon.hive.metastore.client.CloseableMetaStoreClient;

public class MetadataUpdateReplicaLocationManager implements ReplicaLocationManager {

  private final CloseableMetaStoreClient replicaMetastoreClient;
  private final TableType tableType;
  private final String tablePath;
  private final String replicaDatabaseName;
  private final String replicaTableName;

  public MetadataUpdateReplicaLocationManager(
      CloseableMetaStoreClient replicaMetastoreClient,
      TableType tableType,
      String tablePath,
      String replicaDatabaseName,
      String replicaTableName) {
    this.replicaMetastoreClient = replicaMetastoreClient;
    this.tableType = tableType;
    this.tablePath = tablePath;
    this.replicaDatabaseName = replicaDatabaseName;
    this.replicaTableName = replicaTableName;
  }

  @Override
  public Path getTableLocation() {
    return new Path(tablePath);
  }

  @Override
  public Path getPartitionBaseLocation() {
    if (tableType == UNPARTITIONED) {
      throw new UnsupportedOperationException("Not a partitioned table.");
    }
    return getTableLocation();
  }

  @Override
  public void cleanUpLocations() throws CircusTrainException {
    // replicating only meta-data so ignore
  }

  @Override
  public void addCleanUpLocation(String pathEventId, Path location) {
    // replicating only meta-data so ignore
  }

  @Override
  public Path getPartitionLocation(Partition sourcePartition) {
    if (tableType == UNPARTITIONED) {
      throw new UnsupportedOperationException("Not a partitioned table.");
    }
    try {
      Partition partition = replicaMetastoreClient.getPartition(replicaDatabaseName, replicaTableName,
          sourcePartition.getValues());
      // We return the existing replica location so it won't change as we are only updating metadata
      return new Path(partition.getSd().getLocation());
    } catch (TException e) {
      throw new CircusTrainException("Partition should exist on replica but doesn't", e);
    }
  }
}
