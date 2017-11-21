/**
 * Copyright (C) 2016-2017 Expedia Inc.
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
package com.hotels.bdp.circustrain.tool.vacuum;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.Set;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class PartitionedTablePathResolverTest {

  private static final String PARTITION_NAME_1 = "partition=1/x=y";
  private static final String PARTITION_NAME_2 = "partition=2/x=z";
  private static final String PARTITION_TABLE_BASE = "file:///tmp/data";
  private static final String PARTITION_LOCATION_2 = "file:///tmp/data/eventId/year/month/02";
  private static final String PARTITION_LOCATION_1 = "file:///tmp/data/eventId/year/month/01";
  private static final String TABLE_NAME = "table";
  private static final String DATABASE_NAME = "database";
  @Mock
  private IMetaStoreClient metastore;
  @Mock
  private Table table;
  @Mock
  private StorageDescriptor tableSd;
  @Mock
  private StorageDescriptor partitionSd1;
  @Mock
  private StorageDescriptor partitionSd2;
  @Mock
  private Partition partition1;
  @Mock
  private Partition partition2;

  @Before
  public void injectMocks() throws NoSuchObjectException, MetaException, TException {
    when(table.getSd()).thenReturn(tableSd);
    when(table.getDbName()).thenReturn(DATABASE_NAME);
    when(table.getTableName()).thenReturn(TABLE_NAME);
    when(table.getPartitionKeys()).thenReturn(Arrays.asList(new FieldSchema("name", "string", "comments")));
    when(tableSd.getLocation()).thenReturn(PARTITION_TABLE_BASE);
    when(partition1.getSd()).thenReturn(partitionSd1);
    when(partitionSd1.getLocation()).thenReturn(PARTITION_LOCATION_1);
    when(partition2.getSd()).thenReturn(partitionSd2);
    when(partitionSd2.getLocation()).thenReturn(PARTITION_LOCATION_2);
    when(metastore.listPartitionNames(DATABASE_NAME, TABLE_NAME, (short) -1))
        .thenReturn(Arrays.asList(PARTITION_NAME_1, PARTITION_NAME_2));
  }

  @Test
  public void typical() throws Exception {
    when(metastore.listPartitions(DATABASE_NAME, TABLE_NAME, (short) 1)).thenReturn(Arrays.asList(partition1));
    when(metastore.getPartitionsByNames(DATABASE_NAME, TABLE_NAME, Arrays.asList(PARTITION_NAME_1, PARTITION_NAME_2)))
        .thenReturn(Arrays.asList(partition1, partition2));

    TablePathResolver resolver = new PartitionedTablePathResolver(metastore, table);

    Path globPath = resolver.getGlobPath();
    assertThat(globPath, is(new Path("file:///tmp/data/*/*/*/*")));

    Path tableBaseLocation = resolver.getTableBaseLocation();
    assertThat(tableBaseLocation, is(new Path(PARTITION_TABLE_BASE)));

    Set<Path> metastorePaths = resolver.getMetastorePaths((short) 100, 1000);
    assertThat(metastorePaths.size(), is(2));
    assertThat(metastorePaths.contains(new Path(PARTITION_LOCATION_1)), is(true));
    assertThat(metastorePaths.contains(new Path(PARTITION_LOCATION_2)), is(true));
  }

  @Test(expected = ConfigurationException.class)
  public void noPartitions() throws Exception {
    when(metastore.listPartitions(DATABASE_NAME, TABLE_NAME, (short) 1))
        .thenReturn(Collections.<Partition> emptyList());

    new PartitionedTablePathResolver(metastore, table);
  }

}
