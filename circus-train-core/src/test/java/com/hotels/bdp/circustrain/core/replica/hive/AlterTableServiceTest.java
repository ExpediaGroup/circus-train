/**
 * Copyright (C) 2016-2020 Expedia, Inc.
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
package com.hotels.bdp.circustrain.core.replica.hive;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.hotels.hcommon.hive.metastore.client.api.CloseableMetaStoreClient;

@RunWith(MockitoJUnitRunner.class)
public class AlterTableServiceTest {

  private static final String OLD_TABLE_NAME = "old_table";
  private static final String NEW_TABLE_NAME = "new_table";
  private static final String NEW_TABLE_NAME_TEMP = NEW_TABLE_NAME + "_temp";
  private static final String OLD_DB_NAME = "old_db";
  private static final String NEW_DB_NAME = "new_db";

  private @Mock CloseableMetaStoreClient client;
  private @Mock DropTableService dropTableService;
  private @Mock CopyPartitionsOperation copyPartitionsOperation;
  private @Mock RenameTableOperation renameTableOperation;

  private AlterTableService service;
  private Table oldTable = new Table();
  private Table newTable = new Table();

  @Before
  public void setUp() {
    service = new AlterTableService(dropTableService, copyPartitionsOperation, renameTableOperation);
    oldTable.setTableName(OLD_TABLE_NAME);
    newTable.setTableName(NEW_TABLE_NAME);
    oldTable.setDbName(OLD_DB_NAME);
    newTable.setDbName(NEW_DB_NAME);
  }

  @Test
  public void typicalAlterTable() throws Exception {
    oldTable.setSd(createStorageDescriptor(Arrays.asList(new FieldSchema("colA", "string", "some comment"))));
    newTable.setSd(createStorageDescriptor(Arrays.asList(new FieldSchema("colA", "string", "some comment"))));

    service.alterTable(client, oldTable, newTable);

    verify(client).alter_table(NEW_DB_NAME, NEW_TABLE_NAME, newTable);
    verifyNoMoreInteractions(client);
  }

  @Test
  public void alterTableColumnChange() throws Exception {
    oldTable.setSd(createStorageDescriptor(Arrays.asList(new FieldSchema("colA", "string", "some comment"))));
    newTable.setSd(createStorageDescriptor(Arrays.asList(new FieldSchema("colA", "int", "some comment"))));
    Table tempTable = new Table(newTable);
    tempTable.setTableName(NEW_TABLE_NAME_TEMP);

    service.alterTable(client, oldTable, newTable);

    verify(client).createTable(tempTable);
    verify(dropTableService).dropTable(client, NEW_DB_NAME, NEW_TABLE_NAME_TEMP);
    verify(copyPartitionsOperation).execute(client, newTable, tempTable);
    verify(renameTableOperation).execute(client, tempTable, newTable);
    verifyNoMoreInteractions(client);
  }

  @Test
  public void alterTableCopyPartitionsFails() throws Exception {
    oldTable.setSd(createStorageDescriptor(Arrays.asList(new FieldSchema("colA", "string", "some comment"))));
    newTable.setSd(createStorageDescriptor(Arrays.asList(new FieldSchema("colA", "int", "some comment"))));
    Table tempTable = new Table(newTable);
    tempTable.setTableName(NEW_TABLE_NAME_TEMP);
    TException toBeThrown = new TException("error");
    doThrow(toBeThrown).when(copyPartitionsOperation).execute(client, newTable, tempTable);

    try {
      service.alterTable(client, oldTable, newTable);
      fail("Should have thrown exception.");
    } catch (Exception e) {
      verify(client).createTable(tempTable);
      verify(dropTableService).dropTable(client, NEW_DB_NAME, NEW_TABLE_NAME_TEMP);
      verify(copyPartitionsOperation).execute(client, newTable, tempTable);
      verifyZeroInteractions(renameTableOperation);
      verifyNoMoreInteractions(client);
      assertThat(e, is(toBeThrown));
    }
  }

  @Test
  public void alterTableRenameTableFails() throws Exception {
    oldTable.setSd(createStorageDescriptor(Arrays.asList(new FieldSchema("colA", "string", "some comment"))));
    newTable.setSd(createStorageDescriptor(Arrays.asList(new FieldSchema("colA", "int", "some comment"))));
    Table tempTable = new Table(newTable);
    tempTable.setTableName(NEW_TABLE_NAME_TEMP);
    TException toBeThrown = new TException("error");
    doThrow(toBeThrown).when(renameTableOperation).execute(client, tempTable, newTable);

    try {
      service.alterTable(client, oldTable, newTable);
      fail("Should have thrown exception.");
    } catch (Exception e) {
      verify(client).createTable(tempTable);
      verify(dropTableService).dropTable(client, NEW_DB_NAME, NEW_TABLE_NAME_TEMP);
      verify(copyPartitionsOperation).execute(client, newTable, tempTable);
      verify(renameTableOperation).execute(client, tempTable, newTable);
      verifyNoMoreInteractions(client);
      assertThat(e, is(toBeThrown));
    }
  }

  private StorageDescriptor createStorageDescriptor(List<FieldSchema> columns) {
    StorageDescriptor storageDescriptor = new StorageDescriptor();
    storageDescriptor.setCols(columns);
    return storageDescriptor;
  }

}
