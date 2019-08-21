package com.hotels.bdp.circustrain.core.annotation;

import com.hotels.bdp.circustrain.api.conf.OrphanedDataStrategy;
import com.hotels.bdp.circustrain.api.conf.ReplicationMode;

public class TableAnnotatorFactory {

  public static TableAnnotator newInstance(ReplicationMode replicationMode,
    OrphanedDataStrategy orphanedDataStrategy) {
    if (replicationMode == ReplicationMode.FULL) {
      if (orphanedDataStrategy == OrphanedDataStrategy.BEEKEEPER) {
        return new BeekeeperTableAnnotator();
      } else if (orphanedDataStrategy == OrphanedDataStrategy.CUSTOM) {
        return new DefaultTableAnnotator();
      } else {
        return TableAnnotator.NULL_TABLE_ANNOTATOR;
      }
    } else {
      return TableAnnotator.NULL_TABLE_ANNOTATOR;
    }
  }

}
