package com.hotels.bdp.circustrain.core.annotation;

import com.google.common.base.Supplier;

import com.hotels.bdp.circustrain.api.conf.OrphanedDataStrategy;
import com.hotels.bdp.circustrain.api.conf.ReplicationMode;
import com.hotels.hcommon.hive.metastore.client.api.CloseableMetaStoreClient;

public class EventBasedCleanupFactory {

  public static HiveTableAnnotator newInstance(ReplicationMode replicationMode,
    OrphanedDataStrategy orphanedDataStrategy, Supplier<CloseableMetaStoreClient> replicaMetaStoreClientSupplier) {
    if (replicationMode == ReplicationMode.FULL && orphanedDataStrategy == OrphanedDataStrategy.HIVE_HOOK) {
      return new DefaultHiveTableAnnotator(replicaMetaStoreClientSupplier);
    } else {
      return HiveTableAnnotator.NULL_HIVE_TABLE_ANNOTATOR;
    }
  }

}
