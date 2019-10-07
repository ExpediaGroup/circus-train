/**
 * Copyright (C) 2016-2019 Expedia, Inc.
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

import com.hotels.bdp.circustrain.api.CircusTrainException;
import com.hotels.bdp.circustrain.api.ReplicaLocationManager;
import com.hotels.bdp.circustrain.api.SourceLocationManager;
import com.hotels.hcommon.hive.metastore.util.LocationUtils;

public class MetadataMirrorReplicaLocationManager implements ReplicaLocationManager {

  private final SourceLocationManager sourceLocationManager;
  private final TableType tableType;

  public MetadataMirrorReplicaLocationManager(SourceLocationManager sourceLocationManager, TableType tableType) {
    this.sourceLocationManager = sourceLocationManager;
    this.tableType = tableType;
  }

  @Override
  public Path getTableLocation() throws CircusTrainException {
    return sourceLocationManager.getTableLocation();
  }

  @Override
  public Path getPartitionBaseLocation() throws CircusTrainException {
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
    // for views
    if (!LocationUtils.hasLocation(sourcePartition)) {
      return null;
    }
    return new Path(sourcePartition.getSd().getLocation());
  }
}
