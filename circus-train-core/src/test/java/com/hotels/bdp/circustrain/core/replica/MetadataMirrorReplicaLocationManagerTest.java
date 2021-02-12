/**
 * Copyright (C) 2016-2021 Expedia, Inc.
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

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.when;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.hotels.bdp.circustrain.api.SourceLocationManager;

@RunWith(MockitoJUnitRunner.class)
public class MetadataMirrorReplicaLocationManagerTest {

  private static final Path TABLE_LOCATION = new Path("/tmp");
  private static final String PARTITION_LOCATION = "/tmp/key1=value";

  private @Mock SourceLocationManager sourceLocationManager;
  private @Mock Partition sourcePartition;
  private @Mock StorageDescriptor sd;

  @Test
  public void partitionedTableLocation() throws Exception {
    when(sourceLocationManager.getTableLocation()).thenReturn(TABLE_LOCATION);
    when(sourcePartition.getSd()).thenReturn(sd);
    when(sd.getLocation()).thenReturn(PARTITION_LOCATION);

    MetadataMirrorReplicaLocationManager replicaLocationManager = new MetadataMirrorReplicaLocationManager(
        sourceLocationManager, TableType.PARTITIONED);
    assertThat(replicaLocationManager.getTableLocation(), is(TABLE_LOCATION));
    assertThat(replicaLocationManager.getPartitionBaseLocation(), is(TABLE_LOCATION));
    assertThat(replicaLocationManager.getPartitionLocation(sourcePartition), is(new Path(PARTITION_LOCATION)));
  }

  @Test
  public void unpartitionedTableLocation() throws Exception {
    when(sourceLocationManager.getTableLocation()).thenReturn(TABLE_LOCATION);
    MetadataMirrorReplicaLocationManager replicaLocationManager = new MetadataMirrorReplicaLocationManager(
        sourceLocationManager, TableType.UNPARTITIONED);
    assertThat(replicaLocationManager.getTableLocation(), is(TABLE_LOCATION));
  }

  @Test(expected = UnsupportedOperationException.class)
  public void unpartitionedTableLocationThrowsExceptionOnPartitionBaseTableLocation() throws Exception {
    MetadataMirrorReplicaLocationManager replicaLocationManager = new MetadataMirrorReplicaLocationManager(
        sourceLocationManager, TableType.UNPARTITIONED);
    replicaLocationManager.getPartitionBaseLocation();
  }

  @Test(expected = UnsupportedOperationException.class)
  public void unpartitionedTableLocationThrowsExceptionOnPartitionLocation() throws Exception {
    MetadataMirrorReplicaLocationManager replicaLocationManager = new MetadataMirrorReplicaLocationManager(
        sourceLocationManager, TableType.UNPARTITIONED);
    replicaLocationManager.getPartitionLocation(sourcePartition);
  }

  @Test
  public void partitionedTableEmptyLocation() throws Exception {
    when(sourceLocationManager.getTableLocation()).thenReturn(null);
    when(sourcePartition.getSd()).thenReturn(sd);
    when(sd.getLocation()).thenReturn(null);

    MetadataMirrorReplicaLocationManager replicaLocationManager = new MetadataMirrorReplicaLocationManager(
        sourceLocationManager, TableType.PARTITIONED);
    assertThat(replicaLocationManager.getTableLocation(), is(nullValue()));
    assertThat(replicaLocationManager.getPartitionBaseLocation(), is(nullValue()));
    assertThat(replicaLocationManager.getPartitionLocation(sourcePartition), is(nullValue()));
  }

  @Test
  public void unpartitionedTableEmptyLocation() throws Exception {
    when(sourceLocationManager.getTableLocation()).thenReturn(null);
    MetadataMirrorReplicaLocationManager replicaLocationManager = new MetadataMirrorReplicaLocationManager(
        sourceLocationManager, TableType.UNPARTITIONED);
    assertThat(replicaLocationManager.getTableLocation(), is(nullValue()));
  }

}
