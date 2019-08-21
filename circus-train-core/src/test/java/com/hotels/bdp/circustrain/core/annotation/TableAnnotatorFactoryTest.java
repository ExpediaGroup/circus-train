package com.hotels.bdp.circustrain.core.annotation;

import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertThat;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import com.hotels.bdp.circustrain.api.conf.OrphanedDataStrategy;
import com.hotels.bdp.circustrain.api.conf.ReplicationMode;

@RunWith(MockitoJUnitRunner.class)
public class TableAnnotatorFactoryTest {

  @Test
  public void fullReplicationBeekeeperTableAnnotator() {
    TableAnnotator TableAnnotator = TableAnnotatorFactory.newInstance(ReplicationMode.FULL,
      OrphanedDataStrategy.BEEKEEPER);
    assertThat(TableAnnotator, instanceOf(BeekeeperTableAnnotator.class));
  }

  @Test
  public void fullReplicationCustomTableAnnotator() {
    TableAnnotator TableAnnotator = TableAnnotatorFactory.newInstance(ReplicationMode.FULL,
      OrphanedDataStrategy.CUSTOM);
    assertThat(TableAnnotator, instanceOf(DefaultTableAnnotator.class));
  }

  @Test
  public void fullReplicationNoneTableAnnotator() {
    TableAnnotator TableAnnotator = TableAnnotatorFactory.newInstance(ReplicationMode.FULL,
      OrphanedDataStrategy.NONE);
    assertThat(TableAnnotator, instanceOf(TableAnnotator.NULL_TABLE_ANNOTATOR.getClass()));
  }

  @Test
  public void notFullReplicationBeekeeperTableAnnotator() {
    TableAnnotator TableAnnotator = TableAnnotatorFactory.newInstance(
      ReplicationMode.METADATA_MIRROR,
      OrphanedDataStrategy.BEEKEEPER);
    assertThat(TableAnnotator, instanceOf(TableAnnotator.NULL_TABLE_ANNOTATOR.getClass()));
  }

  @Test
  public void notFullReplicationCustomTableAnnotator() {
    TableAnnotator TableAnnotator = TableAnnotatorFactory.newInstance(
      ReplicationMode.METADATA_MIRROR,
      OrphanedDataStrategy.CUSTOM);
    assertThat(TableAnnotator, instanceOf(TableAnnotator.NULL_TABLE_ANNOTATOR.getClass()));
  }

  @Test
  public void notFullReplicationNoneTableAnnotator() {
    TableAnnotator TableAnnotator = TableAnnotatorFactory.newInstance(
      ReplicationMode.METADATA_MIRROR,
      OrphanedDataStrategy.NONE);
    assertThat(TableAnnotator, instanceOf(TableAnnotator.NULL_TABLE_ANNOTATOR.getClass()));
  }

}
