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

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hotels.hcommon.hive.metastore.client.api.CloseableMetaStoreClient;

public class AlterTableService {

  private static final Logger LOG = LoggerFactory.getLogger(AlterTableService.class);

  private DropTableService dropTableService;
  private CopyPartitionsOperation copyPartitionsOperation;
  private RenameTableOperation renameTableOperation;

  public AlterTableService(
      DropTableService dropTableService,
      CopyPartitionsOperation copyPartitionsOperation,
      RenameTableOperation renameTableOperation) {
    this.dropTableService = dropTableService;
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
      String tempName = newTable.getTableName() + "_temp";
      tempTable.setTableName(tempName);
      try {
        client.createTable(tempTable);
        copyPartitionsOperation.execute(client, newTable, tempTable);
        renameTableOperation.execute(client, tempTable, newTable);
      } finally {
        dropTableService.removeCustomParamsAndDrop(client, tempTable.getDbName(), tempName);
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
