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
  private static final String TO_TABLE_NAME_TEMP = TO_TABLE_NAME + "_original";
  private static final String FROM_DB_NAME = "old_db";
  private static final String TO_DB_NAME = "new_db";

  private @Mock CloseableMetaStoreClient client;
  private Table fromTable = new Table();
  private Table toTable = new Table();
  private RenameTableOperation operation;

  @Before
  public void setUp() throws TException {
    operation = new RenameTableOperation();
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
    verify(client).dropTable(TO_DB_NAME, TO_TABLE_NAME_TEMP, false, true);
  }

  @Test
  public void renameTableExceptionThrown1() throws Exception {
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
      verify(client).dropTable(TO_DB_NAME, TO_TABLE_NAME_TEMP, false, true);
      verifyNoMoreInteractions(client);
      assertThat(e, is(toBeThrown));
    }
  }

  @Test
  public void renameTableExceptionThrown2() throws Exception {
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
      verify(client).dropTable(TO_DB_NAME, TO_TABLE_NAME_TEMP, false, true);
      verifyNoMoreInteractions(client);
      assertThat(e, is(toBeThrown));
    }
  }

}
