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

import com.hotels.bdp.circustrain.api.data.DataManipulator;
import com.hotels.hcommon.hive.metastore.client.api.CloseableMetaStoreClient;
import com.hotels.hcommon.hive.metastore.iterator.PartitionIterator;

public class DropTableService {

  private static final Logger log = LoggerFactory.getLogger(DropTableService.class);
  private static final String EXTERNAL_KEY = "EXTERNAL";
  private static final String IS_EXTERNAL = "TRUE";
  private static final short BATCH_SIZE = (short) 10;

  /**
   * Removes all parameters from a table before dropping the table.
   * 
   * @throws Exception if the table can't be deleted.
   */
  public void dropTable(CloseableMetaStoreClient client, String databaseName, String tableName) throws Exception {
    Table table = getTable(client, databaseName, tableName);
    if (table != null) {
      removeTableParamsAndDrop(client, table, databaseName, tableName);
    }
  }

  /**
   * Drops the table and its associated data. If the table is unpartitioned the table location is used. If the table is
   * partitioned then the data will be dropped from each partition location.
   * 
   * @throws Exception if the table or its data can't be deleted.
   */
  public void dropTableAndData(
      CloseableMetaStoreClient client,
      String databaseName,
      String tableName,
      DataManipulator dataManipulator)
    throws Exception
  {
    log.debug("Dropping table {}.{} and its data.", databaseName, tableName);
    Table table = getTable(client, databaseName, tableName);

    if (table != null) {
      String replicaLocation = table.getSd().getLocation();
      if (table.getPartitionKeysSize() == 0) {
        deleteData(dataManipulator, replicaLocation);
      } else {
        deletePartitionData(client, table, dataManipulator);
      }
      removeTableParamsAndDrop(client, table, databaseName, tableName);
    }
  }

  /**
   * Removes all parameters from a table before dropping the table.
   */
  private void removeTableParamsAndDrop(
      CloseableMetaStoreClient client,
      Table table,
      String databaseName,
      String tableName)
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
    log.info("Dropping table '{}.{}'.", databaseName, tableName);
    client.dropTable(databaseName, tableName, false, true);
  }

  private void deleteData(DataManipulator dataManipulator, String replicaDataLocation) throws Exception {
    try {
      log.debug("Deleting table data from location: {}.", replicaDataLocation);
      dataManipulator.delete(replicaDataLocation);
    } catch (IOException e) {
      throw new Exception("Error deleting data for existing replica table.", e);
    }
  }

  private void deletePartitionData(CloseableMetaStoreClient client, Table table, DataManipulator dataManipulator)
    throws Exception {
    String location;
    PartitionIterator partitionIterator = new PartitionIterator(client, table, BATCH_SIZE);
    while (partitionIterator.hasNext()) {
      location = partitionIterator.next().getSd().getLocation();
      deleteData(dataManipulator, location);
    }
  }

  private Table getTable(CloseableMetaStoreClient client, String databaseName, String tableName)
    throws Exception {
    Table table = null;
    try {
      table = client.getTable(databaseName, tableName);
    } catch (NoSuchObjectException e) {
      log.info("No replica table '" + databaseName + "." + tableName + "' found. Nothing to delete.");
    }
    return table;
  }

  private boolean isExternal(Map<String, String> tableParameters) {
    CaseInsensitiveMap caseInsensitiveParams = new CaseInsensitiveMap(tableParameters);
    return IS_EXTERNAL.equalsIgnoreCase((String) caseInsensitiveParams.get(EXTERNAL_KEY));
  }

}
