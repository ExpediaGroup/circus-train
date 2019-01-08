/**
 * Copyright (C) 2016-2018 Expedia Inc.
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
package com.hotels.bdp.circustrain.core.source;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.base.Supplier;
import com.google.common.collect.Lists;

import com.hotels.bdp.circustrain.api.conf.SourceTable;
import com.hotels.bdp.circustrain.api.conf.TableReplication;
import com.hotels.hcommon.hive.metastore.client.api.CloseableMetaStoreClient;

@RunWith(MockitoJUnitRunner.class)
public class DestructiveSourceTest {

  private static final String DATABASE = "db";
  private static final String TABLE = "table1";

  private @Mock Supplier<CloseableMetaStoreClient> sourceMetaStoreClientSupplier;
  private @Mock CloseableMetaStoreClient client;

  private final TableReplication tableReplication = new TableReplication();

  @Before
  public void setUp() {
    SourceTable sourceTable = new SourceTable();
    sourceTable.setDatabaseName(DATABASE);
    sourceTable.setTableName(TABLE);
    tableReplication.setSourceTable(sourceTable);
    when(sourceMetaStoreClientSupplier.get()).thenReturn(client);
  }

  @Test
  public void tableExists() throws Exception {
    when(client.tableExists(DATABASE, TABLE)).thenReturn(true);

    DestructiveSource source = new DestructiveSource(sourceMetaStoreClientSupplier, tableReplication);
    assertThat(source.tableExists(), is(true));
    verify(client).close();
  }

  @Test
  public void getPartitionNames() throws Exception {
    List<String> expectedPartitionNames = Lists.newArrayList("partition1=value1");
    when(client.listPartitionNames(DATABASE, TABLE, (short) -1)).thenReturn(expectedPartitionNames);

    DestructiveSource source = new DestructiveSource(sourceMetaStoreClientSupplier, tableReplication);
    assertThat(source.getPartitionNames(), is(expectedPartitionNames));
    verify(client).close();
  }

}
