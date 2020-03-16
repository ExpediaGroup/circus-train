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

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

import com.hotels.hcommon.hive.metastore.client.api.CloseableMetaStoreClient;
import com.hotels.hcommon.hive.metastore.iterator.PartitionIterator;

public class CopyPartitionsOperation {

  private static final Logger LOG = LoggerFactory.getLogger(CopyPartitionsOperation.class);
  private static final short DEFAULT_BATCH_SIZE = 1000;

  private short partitionBatchSize;

  public CopyPartitionsOperation() {
    this(DEFAULT_BATCH_SIZE);
  }

  @VisibleForTesting
  CopyPartitionsOperation(short partitionBatchSize) {
    this.partitionBatchSize = partitionBatchSize;
  }

  /**
   * Copies partitions from oldTable to newTable, partitions copied are modified to take the schema of newTable
   */
  public void execute(CloseableMetaStoreClient client, Table oldTable, Table newTable) throws TException {
    int count = 0;
    String databaseName = newTable.getDbName();
    String tableName = newTable.getTableName();
    PartitionIterator partitionIterator = new PartitionIterator(client, oldTable, partitionBatchSize);
    while (partitionIterator.hasNext()) {
      List<Partition> batch = new ArrayList<>();
      for (int i = 0; i < partitionBatchSize && partitionIterator.hasNext(); i++) {
        Partition partition = partitionIterator.next();
        count++;
        Partition copy = new Partition(partition);
        copy.setDbName(databaseName);
        copy.setTableName(tableName);
        StorageDescriptor sd = new StorageDescriptor(partition.getSd());
        sd.setCols(newTable.getSd().getCols());
        copy.setSd(sd);
        batch.add(copy);
      }
      LOG.info("Copying batch of size {} to {}.{}", batch.size(), databaseName, tableName);
      client.add_partitions(batch);
    }
    LOG.info("Copied {} partitions to {}.{}", count, databaseName, tableName);
  }

}
