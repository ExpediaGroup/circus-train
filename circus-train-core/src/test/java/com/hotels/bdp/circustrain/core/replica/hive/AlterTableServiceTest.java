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
  private @Mock CopyPartitionsOperation copyPartitionsOperation;
  private @Mock RenameTableOperation renameTableOperation;

  private AlterTableService service;
  private Table oldTable = new Table();
  private Table newTable = new Table();

  @Before
  public void setUp() {
    service = new AlterTableService(copyPartitionsOperation, renameTableOperation);
    oldTable.setTableName(OLD_TABLE_NAME);
    newTable.setTableName(NEW_TABLE_NAME);
    oldTable.setDbName(OLD_DB_NAME);
    newTable.setDbName(NEW_DB_NAME);
  }

  @Test
  public void typicalAlterTable() throws TException {
    oldTable.setSd(getSd(Arrays.asList(new FieldSchema("colA", "string", "some comment"))));
    newTable.setSd(getSd(Arrays.asList(new FieldSchema("colA", "string", "some comment"))));

    service.alterTable(client, oldTable, newTable);

    verify(client).alter_table(NEW_DB_NAME, NEW_TABLE_NAME, newTable);
    verifyNoMoreInteractions(client);
  }

  @Test
  public void alterTableColumnChange() throws TException {
    oldTable.setSd(getSd(Arrays.asList(new FieldSchema("colA", "string", "some comment"))));
    newTable.setSd(getSd(Arrays.asList(new FieldSchema("colA", "int", "some comment"))));
    Table tempTable = new Table(newTable);
    tempTable.setTableName(NEW_TABLE_NAME_TEMP);

    service.alterTable(client, oldTable, newTable);

    verify(client).createTable(tempTable);
    verify(client).dropTable(NEW_DB_NAME, NEW_TABLE_NAME_TEMP, false, true);
    verify(copyPartitionsOperation).execute(client, newTable, tempTable);
    verify(renameTableOperation).execute(client, tempTable, newTable);
    verifyNoMoreInteractions(client);
  }

  @Test
  public void alterTableCopyPartitionsFails() throws TException {
    oldTable.setSd(getSd(Arrays.asList(new FieldSchema("colA", "string", "some comment"))));
    newTable.setSd(getSd(Arrays.asList(new FieldSchema("colA", "int", "some comment"))));
    Table tempTable = new Table(newTable);
    tempTable.setTableName(NEW_TABLE_NAME_TEMP);
    TException toBeThrown = new TException("error");
    doThrow(toBeThrown).when(copyPartitionsOperation).execute(client, newTable, tempTable);

    try {
      service.alterTable(client, oldTable, newTable);
      fail("Should have thrown exception.");
    } catch (Exception e) {
      verify(client).createTable(tempTable);
      verify(client).dropTable(NEW_DB_NAME, NEW_TABLE_NAME_TEMP, false, true);
      verify(copyPartitionsOperation).execute(client, newTable, tempTable);
      verifyZeroInteractions(renameTableOperation);
      verifyNoMoreInteractions(client);
      assertThat(e, is(toBeThrown));
    }
  }

  @Test
  public void alterTableRenameTableFails() throws TException {
    oldTable.setSd(getSd(Arrays.asList(new FieldSchema("colA", "string", "some comment"))));
    newTable.setSd(getSd(Arrays.asList(new FieldSchema("colA", "int", "some comment"))));
    Table tempTable = new Table(newTable);
    tempTable.setTableName(NEW_TABLE_NAME_TEMP);
    TException toBeThrown = new TException("error");
    doThrow(toBeThrown).when(renameTableOperation).execute(client, tempTable, newTable);

    try {
      service.alterTable(client, oldTable, newTable);
      fail("Should have thrown exception.");
    } catch (Exception e) {
      verify(client).createTable(tempTable);
      verify(client).dropTable(NEW_DB_NAME, NEW_TABLE_NAME_TEMP, false, true);
      verify(copyPartitionsOperation).execute(client, newTable, tempTable);
      verify(renameTableOperation).execute(client, tempTable, newTable);
      verifyNoMoreInteractions(client);
      assertThat(e, is(toBeThrown));
    }
  }

  private StorageDescriptor getSd(List<FieldSchema> columns) {
    StorageDescriptor storageDescriptor = new StorageDescriptor();
    storageDescriptor.setCols(columns);
    return storageDescriptor;
  }

}
