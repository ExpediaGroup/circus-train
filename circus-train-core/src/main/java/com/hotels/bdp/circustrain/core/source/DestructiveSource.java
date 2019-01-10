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
package com.hotels.bdp.circustrain.core.source;

import java.util.List;

import org.apache.thrift.TException;

import com.google.common.base.Supplier;

import com.hotels.bdp.circustrain.api.conf.TableReplication;
import com.hotels.hcommon.hive.metastore.client.api.CloseableMetaStoreClient;

public class DestructiveSource {

  private static final short ALL = -1;
  private final Supplier<CloseableMetaStoreClient> sourceMetaStoreClientSupplier;
  private final String databaseName;
  private final String tableName;

  public DestructiveSource(
      Supplier<CloseableMetaStoreClient> sourceMetaStoreClientSupplier,
      TableReplication tableReplication) {
    this.sourceMetaStoreClientSupplier = sourceMetaStoreClientSupplier;
    databaseName = tableReplication.getSourceTable().getDatabaseName();
    tableName = tableReplication.getSourceTable().getTableName();
  }

  public boolean tableExists() throws TException {
    try (CloseableMetaStoreClient client = sourceMetaStoreClientSupplier.get()) {
      return client.tableExists(databaseName, tableName);
    }
  }

  public List<String> getPartitionNames() throws TException {
    try (CloseableMetaStoreClient client = sourceMetaStoreClientSupplier.get()) {
      return client.listPartitionNames(databaseName, tableName, ALL);
    }
  }

}
