/**
 * Copyright (C) 2016-2019 Expedia Inc.
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
package com.hotels.bdp.circustrain.core.annotation;

import java.util.Map;

import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;

import com.google.common.base.Supplier;

import com.hotels.bdp.circustrain.api.CircusTrainException;
import com.hotels.hcommon.hive.metastore.client.api.CloseableMetaStoreClient;

public class BeekeeperHiveTableAnnotator implements HiveTableAnnotator {

  protected static final String BEEKEEPER_PARAM_KEY = "beekeeper.remove.unreferenced.data";
  protected static final String BEEKEEPER_PARAM_VALUE_ENABLE = "true";
  private Supplier<CloseableMetaStoreClient> replicaMetaStoreClientSupplier;

  public BeekeeperHiveTableAnnotator(Supplier<CloseableMetaStoreClient> replicaMetaStoreClientSupplier) {
    this.replicaMetaStoreClientSupplier = replicaMetaStoreClientSupplier;
  }

  @Override
  public void annotateTable(String databaseName, String tableName, Map<String, String> properties)
    throws CircusTrainException {
    try (CloseableMetaStoreClient client = replicaMetaStoreClientSupplier.get()) {
      Table table = client.getTable(databaseName, tableName);
      Map<String, String> parameters = table.getParameters();
      parameters.put(BEEKEEPER_PARAM_KEY, BEEKEEPER_PARAM_VALUE_ENABLE);
      parameters.putAll(properties);
      table.setParameters(parameters);
      client.alter_table(databaseName, tableName, table);
    } catch (TException e) {
      throw new CircusTrainException(String.format("Unable to add properties to table"), e);
    }
  }
}
