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
import static org.mockito.Mockito.when;

import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.hotels.hcommon.hive.metastore.client.api.CloseableMetaStoreClient;

@RunWith(MockitoJUnitRunner.class)
public class RenameTableOperationTest {

  private static final String FROM_TABLE_NAME = "old_table";
  private static final String TO_TABLE_NAME = "new_table";
  private static final String TO_TABLE_NAME_TEMP = TO_TABLE_NAME + "_delete_me";
  private static final String FROM_DB_NAME = "old_db";
  private static final String TO_DB_NAME = "new_db";

  private @Mock CloseableMetaStoreClient client;
  private @Mock DropTableService dropTableService;
  private Table fromTable = new Table();
  private Table toTable = new Table();
  private RenameTableOperation operation;

  @Before
  public void setUp() throws TException {
    operation = new RenameTableOperation(dropTableService);
    fromTable.setTableName(FROM_TABLE_NAME);
    toTable.setTableName(TO_TABLE_NAME);
    fromTable.setDbName(FROM_DB_NAME);
    toTable.setDbName(TO_DB_NAME);
    when(client.getTable(FROM_DB_NAME, FROM_TABLE_NAME)).thenReturn(fromTable);
    when(client.getTable(TO_DB_NAME, TO_TABLE_NAME)).thenReturn(toTable);
  }

  @Test
  public void typicalRenameTable() throws Exception {
    Table toTableTemp = new Table(toTable);
    toTableTemp.setTableName(TO_TABLE_NAME_TEMP);

    operation.execute(client, fromTable, toTable);

    fromTable.setTableName(TO_TABLE_NAME);
    verify(client).alter_table(TO_DB_NAME, TO_TABLE_NAME, toTableTemp);
    verify(client).alter_table(FROM_DB_NAME, FROM_TABLE_NAME, fromTable);
    verify(dropTableService).dropTable(client, TO_DB_NAME, TO_TABLE_NAME_TEMP);
  }

  @Test
  public void renameToTableException() throws Exception {
    Table toTableTemp = new Table(toTable);
    toTableTemp.setTableName(TO_TABLE_NAME_TEMP);
    TException toBeThrown = new TException();
    doThrow(toBeThrown).when(client).alter_table(TO_DB_NAME, TO_TABLE_NAME, toTableTemp);

    try {
      operation.execute(client, fromTable, toTable);
      fail("Should throw exception.");
    } catch (Exception e) {
      verify(client).getTable(FROM_DB_NAME, FROM_TABLE_NAME);
      verify(client).getTable(TO_DB_NAME, TO_TABLE_NAME);
      fromTable.setTableName(TO_TABLE_NAME);
      verify(client).alter_table(TO_DB_NAME, TO_TABLE_NAME, toTableTemp);
      verify(dropTableService).dropTable(client, TO_DB_NAME, TO_TABLE_NAME_TEMP);
      verifyNoMoreInteractions(client);
      assertThat(e, is(toBeThrown));
    }
  }

  @Test
  public void renameFromTableException() throws Exception {
    Table toTableTemp = new Table(toTable);
    toTableTemp.setTableName(TO_TABLE_NAME_TEMP);
    TException toBeThrown = new TException();
    doThrow(toBeThrown).when(client).alter_table(FROM_DB_NAME, FROM_TABLE_NAME, fromTable);

    try {
      operation.execute(client, fromTable, toTable);
      fail("Should throw exception.");
    } catch (Exception e) {
      verify(client).getTable(FROM_DB_NAME, FROM_TABLE_NAME);
      verify(client).getTable(TO_DB_NAME, TO_TABLE_NAME);
      fromTable.setTableName(TO_TABLE_NAME);
      verify(client).alter_table(TO_DB_NAME, TO_TABLE_NAME, toTableTemp);
      verify(client).alter_table(FROM_DB_NAME, FROM_TABLE_NAME, fromTable);
      verify(dropTableService).dropTable(client, TO_DB_NAME, TO_TABLE_NAME_TEMP);
      verifyNoMoreInteractions(client);
      assertThat(e, is(toBeThrown));
    }
  }

}
