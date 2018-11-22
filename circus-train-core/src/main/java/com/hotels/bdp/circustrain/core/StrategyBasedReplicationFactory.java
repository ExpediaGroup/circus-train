package com.hotels.bdp.circustrain.core;

import com.google.common.base.Supplier;

import com.hotels.bdp.circustrain.api.Replication;
import com.hotels.bdp.circustrain.api.conf.ReplicationMode;
import com.hotels.bdp.circustrain.api.conf.ReplicationStrategy;
import com.hotels.bdp.circustrain.api.conf.TableReplication;
import com.hotels.bdp.circustrain.api.event.ReplicaCatalogListener;
import com.hotels.bdp.circustrain.api.listener.HousekeepingListener;
import com.hotels.bdp.circustrain.core.replica.CleanupLocationManager;
import com.hotels.bdp.circustrain.core.replica.DestructiveReplica;
import com.hotels.bdp.circustrain.core.replica.HouseKeepingCleanupLocationManager;
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
    String eventId = eventIdFactory.newEventId("ctp");
    if (tableReplication.getReplicationStrategy() == ReplicationStrategy.DESTRUCTIVE) {
      return new DestructiveReplication(upsertReplicationFactory, tableReplication, eventId,
          new DestructiveSource(sourceMetaStoreClientSupplier, tableReplication),
          new DestructiveReplica(replicaMetaStoreClientSupplier,
              getCleanUpLocationManager(eventId, tableReplication.getReplicationMode()), tableReplication));
    }
    return upsertReplicationFactory.newInstance(tableReplication);
  }

  private CleanupLocationManager getCleanUpLocationManager(String eventId, ReplicationMode replicationMode) {
    if (replicationMode == ReplicationMode.FULL) {
      return new HouseKeepingCleanupLocationManager(eventId, housekeepingListener, replicaCatalogListener);
    } else {
      return CleanupLocationManager.NULL_CLEANUP_LOCATION_MANAGER;
    }
  }

}
