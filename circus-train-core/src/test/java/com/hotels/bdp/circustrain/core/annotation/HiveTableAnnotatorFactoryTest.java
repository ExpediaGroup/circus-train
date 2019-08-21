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
public class HiveTableAnnotatorFactoryTest {

  private @Mock Supplier<CloseableMetaStoreClient> clientSupplier;

  @Test
  public void fullReplicationBeekeeperHiveTableAnnotator() {
    HiveTableAnnotator hiveTableAnnotator = HiveTableAnnotatorFactory.newInstance(ReplicationMode.FULL,
      OrphanedDataStrategy.BEEKEEPER,
      clientSupplier);
    assertThat(hiveTableAnnotator, instanceOf(BeekeeperHiveTableAnnotator.class));
  }

  @Test
  public void fullReplicationCustomHiveTableAnnotator() {
    HiveTableAnnotator hiveTableAnnotator = HiveTableAnnotatorFactory.newInstance(ReplicationMode.FULL,
      OrphanedDataStrategy.CUSTOM,
      clientSupplier);
    assertThat(hiveTableAnnotator, instanceOf(DefaultHiveTableAnnotator.class));
  }

  @Test
  public void fullReplicationNoneHiveTableAnnotator() {
    HiveTableAnnotator hiveTableAnnotator = HiveTableAnnotatorFactory.newInstance(ReplicationMode.FULL,
      OrphanedDataStrategy.NONE,
      clientSupplier);
    assertThat(hiveTableAnnotator, instanceOf(HiveTableAnnotator.NULL_HIVE_TABLE_ANNOTATOR.getClass()));
  }

  @Test
  public void notFullReplicationBeekeeperHiveTableAnnotator() {
    HiveTableAnnotator hiveTableAnnotator = HiveTableAnnotatorFactory.newInstance(
      ReplicationMode.METADATA_MIRROR,
      OrphanedDataStrategy.BEEKEEPER,
      clientSupplier);
    assertThat(hiveTableAnnotator, instanceOf(HiveTableAnnotator.NULL_HIVE_TABLE_ANNOTATOR.getClass()));
  }

  @Test
  public void notFullReplicationCustomHiveTableAnnotator() {
    HiveTableAnnotator hiveTableAnnotator = HiveTableAnnotatorFactory.newInstance(
      ReplicationMode.METADATA_MIRROR,
      OrphanedDataStrategy.CUSTOM,
      clientSupplier);
    assertThat(hiveTableAnnotator, instanceOf(HiveTableAnnotator.NULL_HIVE_TABLE_ANNOTATOR.getClass()));
  }

  @Test
  public void notFullReplicationNoneHiveTableAnnotator() {
    HiveTableAnnotator hiveTableAnnotator = HiveTableAnnotatorFactory.newInstance(
      ReplicationMode.METADATA_MIRROR,
      OrphanedDataStrategy.NONE,
      clientSupplier);
    assertThat(hiveTableAnnotator, instanceOf(HiveTableAnnotator.NULL_HIVE_TABLE_ANNOTATOR.getClass()));
  }

}
