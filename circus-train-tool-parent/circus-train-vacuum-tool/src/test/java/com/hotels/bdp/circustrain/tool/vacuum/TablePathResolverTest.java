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

import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TablePathResolverTest {

  private static final String TABLE_NAME = "table";
  private static final String DATABASE_NAME = "database";

  @Mock
  private IMetaStoreClient metastore;
  @Mock
  private Table table;
  @Mock
  private StorageDescriptor tableSd;
  @Mock
  private StorageDescriptor partitionSd;
  @Mock
  private Partition partition;

  @Test
  public void unpartitioned() throws Exception {
    when(table.getSd()).thenReturn(tableSd);
    when(table.getDbName()).thenReturn(DATABASE_NAME);
    when(table.getTableName()).thenReturn(TABLE_NAME);
    when(tableSd.getLocation()).thenReturn("file:///tmp/data/eventId");
    TablePathResolver resolver = TablePathResolver.Factory.newTablePathResolver(metastore, table);
    assertThat(resolver instanceof UnpartitionedTablePathResolver, is(true));
  }

  @Test
  public void partitioned() throws Exception {
    when(table.getSd()).thenReturn(tableSd);
    when(table.getDbName()).thenReturn(DATABASE_NAME);
    when(table.getTableName()).thenReturn(TABLE_NAME);
    when(table.getPartitionKeys()).thenReturn(Arrays.asList(new FieldSchema("name", "string", "comments")));
    when(tableSd.getLocation()).thenReturn("file:///tmp/data");
    when(metastore.listPartitions(DATABASE_NAME, TABLE_NAME, (short) 1)).thenReturn(Arrays.asList(partition));
    when(partition.getSd()).thenReturn(partitionSd);
    when(partitionSd.getLocation()).thenReturn("file:///tmp/data/eventId/year/month/day");
    TablePathResolver resolver = TablePathResolver.Factory.newTablePathResolver(metastore, table);
    assertThat(resolver instanceof PartitionedTablePathResolver, is(true));
  }
}
