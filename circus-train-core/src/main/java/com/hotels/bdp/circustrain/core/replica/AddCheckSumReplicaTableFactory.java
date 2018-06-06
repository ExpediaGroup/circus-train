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

import static com.hotels.bdp.circustrain.api.CircusTrainTableParameter.PARTITION_CHECKSUM;
import static com.hotels.hcommon.hive.metastore.util.LocationUtils.locationAsPath;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;

import com.google.common.base.Function;

import com.hotels.bdp.circustrain.api.metadata.ColumnStatisticsTransformation;
import com.hotels.bdp.circustrain.api.metadata.PartitionTransformation;
import com.hotels.bdp.circustrain.api.metadata.TableTransformation;
import com.hotels.bdp.circustrain.core.conf.ReplicationMode;

public class AddCheckSumReplicaTableFactory extends ReplicaTableFactory {

  private final Function<Path, String> checksumFunction;

  AddCheckSumReplicaTableFactory(
      HiveConf sourceHiveConf,
      Function<Path, String> checksumFunction,
      TableTransformation tableTransformation,
      PartitionTransformation partitionTransformation,
      ColumnStatisticsTransformation columnStatisticsTransformation) {
    super(sourceHiveConf, tableTransformation, partitionTransformation, columnStatisticsTransformation);
    this.checksumFunction = checksumFunction;
  }

  @Override
  Partition newReplicaPartition(
      String eventId,
      Table sourceTable,
      Partition sourcePartition,
      String replicaDatabaseName,
      String replicaTableName,
      Path replicaPartitionLocation,
      ReplicationMode replicationMode) {
    Partition replica = super.newReplicaPartition(eventId, sourceTable, sourcePartition, replicaDatabaseName,
        replicaTableName, replicaPartitionLocation, replicationMode);
    String checksum = checksumFunction.apply(locationAsPath(sourcePartition));
    replica.putToParameters(PARTITION_CHECKSUM.parameterName(), checksum);
    return replica;
  }
}
