/**
 * Copyright (C) 2016-2019 Expedia Inc.
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

import com.google.common.base.Supplier;

import com.hotels.bdp.circustrain.api.Replication;
import com.hotels.bdp.circustrain.api.conf.ReplicationStrategy;
import com.hotels.bdp.circustrain.api.conf.TableReplication;
import com.hotels.bdp.circustrain.api.event.ReplicaCatalogListener;
import com.hotels.bdp.circustrain.api.listener.HousekeepingListener;
import com.hotels.bdp.circustrain.core.replica.CleanupLocationManager;
import com.hotels.bdp.circustrain.core.replica.CleanupLocationManagerFactory;
import com.hotels.bdp.circustrain.core.replica.DestructiveReplica;
import com.hotels.bdp.circustrain.core.source.DestructiveSource;
import com.hotels.hcommon.hive.metastore.client.api.CloseableMetaStoreClient;

public class StrategyBasedReplicationFactory implements ReplicationFactory {

  private final EventIdFactory eventIdFactory = EventIdFactory.DEFAULT;
  private final ReplicationFactoryImpl upsertReplicationFactory;
  private final Supplier<CloseableMetaStoreClient> sourceMetaStoreClientSupplier;
  private final Supplier<CloseableMetaStoreClient> replicaMetaStoreClientSupplier;
  private final HousekeepingListener housekeepingListener;
  private final ReplicaCatalogListener replicaCatalogListener;

  public StrategyBasedReplicationFactory(
      ReplicationFactoryImpl upsertReplicationFactory,
      Supplier<CloseableMetaStoreClient> sourceMetaStoreClientSupplier,
      Supplier<CloseableMetaStoreClient> replicaMetaStoreClientSupplier,
      HousekeepingListener housekeepingListener,
      ReplicaCatalogListener replicaCatalogListener) {
    this.upsertReplicationFactory = upsertReplicationFactory;
    this.sourceMetaStoreClientSupplier = sourceMetaStoreClientSupplier;
    this.replicaMetaStoreClientSupplier = replicaMetaStoreClientSupplier;
    this.housekeepingListener = housekeepingListener;
    this.replicaCatalogListener = replicaCatalogListener;
  }

  @Override
  public Replication newInstance(TableReplication tableReplication) {
    if (tableReplication.getReplicationStrategy() == ReplicationStrategy.PROPAGATE_DELETES) {
      String eventId = eventIdFactory.newEventId(EventIdPrefix.CIRCUS_TRAIN_DESTRUCTIVE.getPrefix());
      CleanupLocationManager cleanupLocationManager = CleanupLocationManagerFactory.newInstance(eventId,
        housekeepingListener, replicaCatalogListener, tableReplication);

      return new DestructiveReplication(upsertReplicationFactory, tableReplication, eventId,
          new DestructiveSource(sourceMetaStoreClientSupplier, tableReplication),
          new DestructiveReplica(replicaMetaStoreClientSupplier, cleanupLocationManager, tableReplication));
    }
    return upsertReplicationFactory.newInstance(tableReplication);
  }
}
