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

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import org.apache.commons.collections.map.CaseInsensitiveMap;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hotels.bdp.circustrain.core.client.DataManipulationClient;
import com.hotels.bdp.circustrain.core.client.DataManipulationClientFactoryManager;
import com.hotels.hcommon.hive.metastore.client.api.CloseableMetaStoreClient;

public class DropTableService {

  private static final Logger LOG = LoggerFactory.getLogger(DropTableService.class);
  private static final String EXTERNAL_KEY = "EXTERNAL";
  private static final String IS_EXTERNAL = "TRUE";

  /**
   * Removes all parameters from a table before dropping the table.
   */
  public Table removeTableParamsAndDrop(CloseableMetaStoreClient client, String databaseName, String tableName)
    throws TException {
    Table table;
    try {
      table = client.getTable(databaseName, tableName);
    } catch (NoSuchObjectException e) {
      LOG.info("No replica table '" + databaseName + "." + tableName + "' found. Nothing to delete.");
      return null;
    }
    dropTable(client, table, databaseName, tableName);
    return table;
  }

  public void dropTableAndData(
      CloseableMetaStoreClient client,
      String databaseName,
      String tableName,
      DataManipulationClientFactoryManager dataManipulationClientFactoryManager)
    throws TException {

    LOG.debug("Dropping table {}.{} and its data.", databaseName, tableName);
    Table table = removeTableParamsAndDrop(client, databaseName, tableName);
    if (table != null) {
      deleteData(dataManipulationClientFactoryManager, table);
    }
  }

  private void dropTable(CloseableMetaStoreClient client, Table table, String databaseName, String tableName)
    throws TException {
    Map<String, String> tableParameters = table.getParameters();
    if (tableParameters != null && !tableParameters.isEmpty()) {
      if (isExternal(tableParameters)) {
        table.setParameters(Collections.singletonMap(EXTERNAL_KEY, IS_EXTERNAL));
      } else {
        table.setParameters(Collections.emptyMap());
      }
      client.alter_table(databaseName, tableName, table);
    }
    LOG.info("Dropping table '{}.{}'.", databaseName, tableName);
    client.dropTable(databaseName, tableName, false, true);
  }

  private void deleteData(DataManipulationClientFactoryManager dataManipulationClientFactoryManager, Table table) {
    String replicaTableLocation = table.getSd().getLocation();

    try {
      LOG.info("Dropping table data from location: {}", replicaTableLocation);

      DataManipulationClient client = dataManipulationClientFactoryManager
          .getClientForPath(replicaTableLocation);

      boolean dataDeleted = client.delete(replicaTableLocation);
      LOG.info("Data deleted: {}", dataDeleted);
    } catch (IOException e) {
      LOG.info("Could not drop replica table data at location:{}.", replicaTableLocation);
    }
  }

  private boolean isExternal(Map<String, String> tableParameters) {
    CaseInsensitiveMap caseInsensitiveParams = new CaseInsensitiveMap(tableParameters);
    return IS_EXTERNAL.equalsIgnoreCase((String) caseInsensitiveParams.get(EXTERNAL_KEY));
  }
}
