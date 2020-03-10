package com.hotels.bdp.circustrain.core.replica.hive;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hotels.bdp.circustrain.core.replica.Replica;
import com.hotels.hcommon.hive.metastore.client.api.CloseableMetaStoreClient;

public class AlterTableService {

  private static final Logger LOG = LoggerFactory.getLogger(Replica.class);

  private CopyPartitionsOperation copyPartitionsOperation;
  private RenameTableOperation renameTableOperation;

  public AlterTableService(
      CopyPartitionsOperation copyPartitionsOperation,
      RenameTableOperation renameTableOperation) {
    this.copyPartitionsOperation = copyPartitionsOperation;
    this.renameTableOperation = renameTableOperation;
  }

  public void alterTable(CloseableMetaStoreClient client, Table oldTable, Table newTable) throws TException {
    List<FieldSchema> oldColumns = oldTable.getSd().getCols();
    List<FieldSchema> newColumns = newTable.getSd().getCols();
    if (hasAnyChangedColumns(oldColumns, newColumns)) {
      LOG
          .info("Found columns that have changed type, attempting to recreate target table with the new columns."
              + "Old columns: {}, new columns: {}", oldColumns, newColumns);
      Table tempTable = new Table(newTable);
      tempTable.setTableName(newTable.getTableName() + "_temp");
      try {
        client.createTable(tempTable);
        copyPartitionsOperation.execute(client, newTable, tempTable);
        renameTableOperation.execute(client, tempTable, newTable);
      } finally {
        client.dropTable(tempTable.getDbName(), tempTable.getTableName(), false, true);
      }
    } else {
      client.alter_table(newTable.getDbName(), newTable.getTableName(), newTable);
    }
  }

  private boolean hasAnyChangedColumns(List<FieldSchema> oldColumns, List<FieldSchema> newColumns) {
    Map<String, FieldSchema> oldColumnsMap = oldColumns.stream()
        .collect(Collectors.toMap(FieldSchema::getName, Function.identity()));
    for (FieldSchema newColumn : newColumns) {
      if (oldColumnsMap.containsKey(newColumn.getName())) {
        FieldSchema oldColumn = oldColumnsMap.get(newColumn.getName());
        if (!oldColumn.getType().equals(newColumn.getType())) {
          return true;
        }
      }
    }
    return false;
  }
}
