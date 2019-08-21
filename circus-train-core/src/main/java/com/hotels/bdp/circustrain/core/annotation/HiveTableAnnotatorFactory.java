package com.hotels.bdp.circustrain.core.annotation;

import com.google.common.base.Supplier;

import com.hotels.bdp.circustrain.api.conf.OrphanedDataStrategy;
import com.hotels.bdp.circustrain.api.conf.ReplicationMode;
import com.hotels.hcommon.hive.metastore.client.api.CloseableMetaStoreClient;

public class HiveTableAnnotatorFactory {

  public static HiveTableAnnotator newInstance(ReplicationMode replicationMode,
    OrphanedDataStrategy orphanedDataStrategy, Supplier<CloseableMetaStoreClient> replicaMetaStoreClientSupplier) {
    if (replicationMode == ReplicationMode.FULL) {
      if (orphanedDataStrategy == OrphanedDataStrategy.BEEKEEPER) {
        return new BeekeeperHiveTableAnnotator(replicaMetaStoreClientSupplier);
      } else if (orphanedDataStrategy == OrphanedDataStrategy.CUSTOM) {
        return new DefaultHiveTableAnnotator(replicaMetaStoreClientSupplier);
      } else {
        return HiveTableAnnotator.NULL_HIVE_TABLE_ANNOTATOR;
      }
    } else {
      return HiveTableAnnotator.NULL_HIVE_TABLE_ANNOTATOR;
    }
  }

}
