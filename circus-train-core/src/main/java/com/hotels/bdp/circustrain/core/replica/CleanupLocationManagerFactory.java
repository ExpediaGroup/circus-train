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

import com.hotels.bdp.circustrain.api.conf.OrphanedDataStrategy;
import com.hotels.bdp.circustrain.api.conf.ReplicationMode;
import com.hotels.bdp.circustrain.api.conf.TableReplication;
import com.hotels.bdp.circustrain.api.event.ReplicaCatalogListener;
import com.hotels.bdp.circustrain.api.listener.HousekeepingListener;

public class CleanupLocationManagerFactory {

  public static CleanupLocationManager newInstance(String eventId, HousekeepingListener housekeepingListener,
    ReplicaCatalogListener replicaCatalogListener, TableReplication tableReplication) {
    if (tableReplication.getReplicationMode() == ReplicationMode.FULL &&
      tableReplication.getOrphanedDataStrategy() == OrphanedDataStrategy.HOUSEKEEPING) {
      return new HousekeepingCleanupLocationManager(eventId, housekeepingListener, replicaCatalogListener,
        tableReplication.getReplicaDatabaseName(), tableReplication.getReplicaTableName());
    } else {
      return CleanupLocationManager.NULL_CLEANUP_LOCATION_MANAGER;
    }
  }

}
