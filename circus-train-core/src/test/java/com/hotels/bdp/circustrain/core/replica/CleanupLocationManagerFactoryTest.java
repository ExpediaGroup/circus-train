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

import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertThat;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.hotels.bdp.circustrain.api.conf.OrphanedDataStrategy;
import com.hotels.bdp.circustrain.api.conf.ReplicaTable;
import com.hotels.bdp.circustrain.api.conf.ReplicationMode;
import com.hotels.bdp.circustrain.api.conf.TableReplication;
import com.hotels.bdp.circustrain.api.event.ReplicaCatalogListener;
import com.hotels.bdp.circustrain.api.listener.HousekeepingListener;

@RunWith(MockitoJUnitRunner.class)
public class CleanupLocationManagerFactoryTest {

  private @Mock HousekeepingListener housekeepingListener;
  private @Mock ReplicaCatalogListener replicaCatalogListener;
  private String eventId = "eventId";

  @Test
  public void typicalHousekeepingCleanupLocationManager() {
    ReplicaTable table = new ReplicaTable();
    table.setDatabaseName("database");
    table.setTableName("table");

    TableReplication tableReplication = new TableReplication();
    tableReplication.setReplicaTable(table);
    tableReplication.setReplicationMode(ReplicationMode.FULL);
    tableReplication.setOrphanedDataStrategy(OrphanedDataStrategy.HOUSEKEEPING);

    CleanupLocationManager cleanupLocationManager = CleanupLocationManagerFactory.newInstance(eventId,
      housekeepingListener, replicaCatalogListener, tableReplication);
    assertThat(cleanupLocationManager, instanceOf(HousekeepingCleanupLocationManager.class));
  }

  @Test
  public void notHousekeepingNullCleanupLocationManager() {
    TableReplication tableReplication = new TableReplication();
    tableReplication.setReplicationMode(ReplicationMode.FULL);
    tableReplication.setOrphanedDataStrategy(OrphanedDataStrategy.NONE);
    CleanupLocationManager cleanupLocationManager = CleanupLocationManagerFactory.newInstance(eventId,
      housekeepingListener, replicaCatalogListener, tableReplication);
    assertThat(cleanupLocationManager, instanceOf(CleanupLocationManager.NULL_CLEANUP_LOCATION_MANAGER.getClass()));
  }

  @Test
  public void metatdataMirrorNullCleanupLocationManager() {
    TableReplication tableReplication = new TableReplication();
    tableReplication.setReplicationMode(ReplicationMode.METADATA_MIRROR);
    tableReplication.setOrphanedDataStrategy(OrphanedDataStrategy.HOUSEKEEPING);
    CleanupLocationManager cleanupLocationManager = CleanupLocationManagerFactory.newInstance(eventId,
      housekeepingListener, replicaCatalogListener, tableReplication);
    assertThat(cleanupLocationManager, instanceOf(CleanupLocationManager.NULL_CLEANUP_LOCATION_MANAGER.getClass()));
  }

  @Test
  public void metatdataUpdateNullCleanupLocationManager() {
    TableReplication tableReplication = new TableReplication();
    tableReplication.setReplicationMode(ReplicationMode.METADATA_UPDATE);
    tableReplication.setOrphanedDataStrategy(OrphanedDataStrategy.HOUSEKEEPING);
    CleanupLocationManager cleanupLocationManager = CleanupLocationManagerFactory.newInstance(eventId,
      housekeepingListener, replicaCatalogListener, tableReplication);
    assertThat(cleanupLocationManager, instanceOf(CleanupLocationManager.NULL_CLEANUP_LOCATION_MANAGER.getClass()));
  }
}
