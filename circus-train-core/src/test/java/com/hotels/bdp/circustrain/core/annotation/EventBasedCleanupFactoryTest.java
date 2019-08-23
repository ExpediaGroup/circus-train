package com.hotels.bdp.circustrain.core.annotation;

import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertThat;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.base.Supplier;

import com.hotels.bdp.circustrain.api.conf.OrphanedDataStrategy;
import com.hotels.bdp.circustrain.api.conf.ReplicationMode;
import com.hotels.hcommon.hive.metastore.client.api.CloseableMetaStoreClient;

@RunWith(MockitoJUnitRunner.class)
public class EventBasedCleanupFactoryTest {

  private @Mock Supplier<CloseableMetaStoreClient> clientSupplier;

  @Test
  public void fullReplicationCustomHiveTableAnnotator() {
    HiveTableAnnotator hiveTableAnnotator = EventBasedCleanupFactory.newInstance(ReplicationMode.FULL,
      OrphanedDataStrategy.HIVE_HOOK,
      clientSupplier);
    assertThat(hiveTableAnnotator, instanceOf(DefaultHiveTableAnnotator.class));
  }

  @Test
  public void fullReplicationNoneHiveTableAnnotator() {
    HiveTableAnnotator hiveTableAnnotator = EventBasedCleanupFactory.newInstance(ReplicationMode.FULL,
      OrphanedDataStrategy.NONE,
      clientSupplier);
    assertThat(hiveTableAnnotator, instanceOf(HiveTableAnnotator.NULL_HIVE_TABLE_ANNOTATOR.getClass()));
  }

  @Test
  public void notFullReplicationCustomHiveTableAnnotator() {
    HiveTableAnnotator hiveTableAnnotator = EventBasedCleanupFactory.newInstance(
      ReplicationMode.METADATA_MIRROR,
      OrphanedDataStrategy.HIVE_HOOK,
      clientSupplier);
    assertThat(hiveTableAnnotator, instanceOf(HiveTableAnnotator.NULL_HIVE_TABLE_ANNOTATOR.getClass()));
  }

  @Test
  public void notFullReplicationNoneHiveTableAnnotator() {
    HiveTableAnnotator hiveTableAnnotator = EventBasedCleanupFactory.newInstance(
      ReplicationMode.METADATA_MIRROR,
      OrphanedDataStrategy.NONE,
      clientSupplier);
    assertThat(hiveTableAnnotator, instanceOf(HiveTableAnnotator.NULL_HIVE_TABLE_ANNOTATOR.getClass()));
  }

}
