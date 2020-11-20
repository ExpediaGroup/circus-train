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

import org.apache.hadoop.hive.metastore.api.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hotels.hcommon.hive.metastore.client.api.CloseableMetaStoreClient;

public class RenameTableOperation {

  private static final Logger LOG = LoggerFactory.getLogger(RenameTableOperation.class);
  private static final String DELETE_ME = "_delete_me";

  private final DropTableService dropTableService;

  public RenameTableOperation(DropTableService dropTableService) {
    this.dropTableService = dropTableService;
  }

  /**
   * <p>
   * NOTE: assumes both `from` and `to` exist
   * </p>
   * Renames tables 'from' table into 'to' table, at the end of the operation 'from' will be gone and 'to' will be
   * renamed.
   */
  public void execute(CloseableMetaStoreClient client, Table from, Table to) throws Exception {
    LOG.info("Renaming table {}.{} to {}.{}", from.getDbName(), from.getTableName(), to.getDbName(), to.getTableName());
    Table fromTable = client.getTable(from.getDbName(), from.getTableName());
    Table toTable = client.getTable(to.getDbName(), to.getTableName());
    String fromDatabaseName = fromTable.getDbName();
    String fromTableName = fromTable.getTableName();
    String toDatabaseName = toTable.getDbName();
    String toTableName = toTable.getTableName();
    String toDelete = toTableName + DELETE_ME;
    try {
      // rename current table to a new intermediate table
      toTable.setTableName(toDelete);
      LOG
          .info("Altering table {}.{} to {}.{}", toDatabaseName, toTableName, toTable.getDbName(),
              toTable.getTableName());
      client.alter_table(toDatabaseName, toTableName, toTable);

      // rename new table to current table
      fromTable.setDbName(toTable.getDbName());
      fromTable.setTableName(toTableName);
      LOG
          .info("Altering table {}.{} to {}.{}", fromDatabaseName, fromTableName, fromTable.getDbName(),
              fromTable.getTableName());
      client.alter_table(fromDatabaseName, fromTableName, fromTable);
    } finally {
      LOG.info("Dropping table {}.{}", toTable.getDbName(), toDelete);
      dropTableService.dropTable(client, toTable.getDbName(), toDelete);
    }
  }
}
