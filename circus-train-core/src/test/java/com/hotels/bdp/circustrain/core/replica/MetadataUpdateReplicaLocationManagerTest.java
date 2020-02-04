/**
 * Copyright (C) 2016-2019 Expedia, Inc.
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
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.collect.Lists;

import com.hotels.hcommon.hive.metastore.client.api.CloseableMetaStoreClient;

@RunWith(MockitoJUnitRunner.class)
public class MetadataUpdateReplicaLocationManagerTest {

  private static final String DATABASE = "replica_db";
  private static final String TABLE = "replica_table";
  private final String tableLocation = "/tmp";
  private final String partitionName = "key1=value";
  private final String partitionLocation = tableLocation + "/" + partitionName;
  private final Path tableLocationPath = new Path("/tmp");
  @Mock
  private Partition sourcePartition;
  @Mock
  private StorageDescriptor sd;
  @Mock
  private CloseableMetaStoreClient client;

  @Before
  public void setUp() throws TException {
    List<String> partitionValues = Lists.newArrayList(partitionName);
    when(sourcePartition.getValues()).thenReturn(partitionValues);
    when(sourcePartition.getSd()).thenReturn(sd);
    when(sd.getLocation()).thenReturn(partitionLocation);
    when(client.getPartition(DATABASE, TABLE, partitionValues)).thenReturn(sourcePartition);
  }

  @Test
  public void partitionedTableLocation() throws Exception {
    MetadataUpdateReplicaLocationManager replicaLocationManager = new MetadataUpdateReplicaLocationManager(client,
        TableType.PARTITIONED, tableLocation, DATABASE, TABLE);
    assertThat(replicaLocationManager.getTableLocation(), is(tableLocationPath));
    assertThat(replicaLocationManager.getPartitionBaseLocation(), is(tableLocationPath));
    assertThat(replicaLocationManager.getPartitionLocation(sourcePartition), is(new Path(partitionLocation)));
  }

  @Test
  public void unpartitionedTableLocation() throws Exception {
    MetadataUpdateReplicaLocationManager replicaLocationManager = new MetadataUpdateReplicaLocationManager(client,
        TableType.UNPARTITIONED, tableLocation, DATABASE, TABLE);
    assertThat(replicaLocationManager.getTableLocation(), is(tableLocationPath));
  }

  @Test(expected = UnsupportedOperationException.class)
  public void unpartitionedTableLocationThrowsExceptionOnPartitionBaseTableLocation() throws Exception {
    MetadataUpdateReplicaLocationManager replicaLocationManager = new MetadataUpdateReplicaLocationManager(client,
        TableType.UNPARTITIONED, tableLocation, DATABASE, TABLE);
    replicaLocationManager.getPartitionBaseLocation();
  }

  @Test(expected = UnsupportedOperationException.class)
  public void unpartitionedTableLocationThrowsExceptionOnPartitionLocation() throws Exception {
    MetadataUpdateReplicaLocationManager replicaLocationManager = new MetadataUpdateReplicaLocationManager(client,
        TableType.UNPARTITIONED, tableLocation, DATABASE, TABLE);
    replicaLocationManager.getPartitionLocation(sourcePartition);
  }

}
