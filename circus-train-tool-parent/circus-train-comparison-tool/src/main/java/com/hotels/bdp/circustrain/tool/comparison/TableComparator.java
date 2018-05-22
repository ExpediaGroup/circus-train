/**
 * Copyright (C) 2016-2018 Expedia Inc.
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
package com.hotels.bdp.circustrain.tool.comparison;

import static java.lang.System.out;

import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;

import com.hotels.bdp.circustrain.api.CircusTrainException;
import com.hotels.bdp.circustrain.comparator.ComparatorRegistry;
import com.hotels.bdp.circustrain.comparator.api.DiffListener;
import com.hotels.bdp.circustrain.comparator.hive.HiveDifferences;
import com.hotels.bdp.circustrain.core.HiveEndpoint;
import com.hotels.bdp.circustrain.core.conf.TableReplication;
import com.hotels.bdp.circustrain.hive.fetcher.BufferedPartitionFetcher;
import com.hotels.bdp.circustrain.hive.fetcher.PartitionFetcher;
import com.hotels.hcommon.hive.metastore.client.api.CloseableMetaStoreClient;
import com.hotels.hcommon.hive.metastore.iterator.PartitionIterator;

class TableComparator {

  private static final Logger LOG = LoggerFactory.getLogger(TableComparator.class);

  private final HiveEndpoint source;
  private final HiveEndpoint replica;
  private final ComparatorRegistry comparatorRegistry;
  private final DiffListener diffListener;

  private final TableReplication tableReplication;

  private final short sourcePartitionBatchSize;

  private final short replicaPartitionBufferSize;

  TableComparator(
      HiveEndpoint source,
      HiveEndpoint replica,
      ComparatorRegistry comparatorRegistry,
      DiffListener diffListener,
      TableReplication tableReplication,
      short sourcePartitionBatchSize,
      short replicaPartitionBufferSize) {
    this.source = source;
    this.replica = replica;
    this.comparatorRegistry = comparatorRegistry;
    this.diffListener = diffListener;
    this.tableReplication = tableReplication;
    this.sourcePartitionBatchSize = sourcePartitionBatchSize;
    this.replicaPartitionBufferSize = replicaPartitionBufferSize;
  }

  public void run() throws CircusTrainException {
    Table sourceTable = source.getTableAndStatistics(tableReplication).getTable();
    Table replicaTable = replica.getTableAndStatistics(tableReplication).getTable();
    out.println(String.format("Source catalog:         %s", source.getName()));
    out.println(String.format("Source MetaStore URIs:  %s", source.getMetaStoreUris()));
    out.println(String.format("Source table:           %s", Warehouse.getQualifiedName(sourceTable)));
    out.println(String.format("Replica catalog:        %s", replica.getName()));
    out.println(String.format("Replica MetaStore URIs: %s", replica.getMetaStoreUris()));
    out.println(String.format("Replica table:          %s", Warehouse.getQualifiedName(replicaTable)));
    out.println();
    out.println();
    try (CloseableMetaStoreClient sourceMetastore = source.getMetaStoreClientSupplier().get()) {
      try (CloseableMetaStoreClient replicaMetastore = replica.getMetaStoreClientSupplier().get()) {
        LOG.info("Computing differences...");
        PartitionIterator partitionIterator = new PartitionIterator(sourceMetastore, sourceTable,
            sourcePartitionBatchSize);
        PartitionFetcher replicaPartitionFetcher = new BufferedPartitionFetcher(replicaMetastore, replicaTable,
            replicaPartitionBufferSize);
        HiveDifferences diffs = HiveDifferences
            .builder(diffListener)
            .comparatorRegistry(comparatorRegistry)
            .source(source.getHiveConf(), sourceTable, partitionIterator)
            .replica(Optional.of(replicaTable), Optional.of(replicaPartitionFetcher))
            .build();
        diffs.run();
      } catch (TException e) {
        throw new CircusTrainException("Could not fetch partitions", e);
      }
    }
  }

}
