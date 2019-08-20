/**
 * Copyright (C) 2016-2017 Expedia Inc.
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
package com.hotels.bdp.circustrain.core.transformation;

import java.util.Map;

import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;

import com.google.common.base.Supplier;

import com.hotels.bdp.circustrain.api.CircusTrainException;
import com.hotels.bdp.circustrain.api.metadata.TableTransformation;
import com.hotels.hcommon.hive.metastore.client.api.CloseableMetaStoreClient;

public class PropertiesTableTransformation implements TableTransformation {

  private Supplier<CloseableMetaStoreClient> replicaMetaStoreClientSupplier;
  private Map<String, String> tableProperties;

  public PropertiesTableTransformation(Supplier<CloseableMetaStoreClient> replicaMetaStoreClientSupplier,
    Map<String, String> tableProperties) {
    this.replicaMetaStoreClientSupplier = replicaMetaStoreClientSupplier;
    this.tableProperties = tableProperties;
  }

  @Override
  public Table transform(Table table) {
    try (CloseableMetaStoreClient client = replicaMetaStoreClientSupplier.get()) {
      Map<String, String> parameters = table.getParameters();
      parameters.putAll(tableProperties);
      table.setParameters(parameters);
      client.alter_table(table.getDbName(), table.getTableName(), table);
    } catch (TException e) {
      throw new CircusTrainException(String.format("Unable to add properties to table"), e);
    }
    return table;
  }
}
