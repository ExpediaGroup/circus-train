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
package com.hotels.bdp.circustrain.hive.fetcher;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

/** Fetches partitions ahead in batches and keeps them in cache until a non-cached partition is requested. */
public class BufferedPartitionFetcher implements PartitionFetcher {

  private static final Logger LOG = LoggerFactory.getLogger(BufferedPartitionFetcher.class);

  private static final short NO_LIMIT = (short) -1;

  private final List<String> partitionNames;
  private final IMetaStoreClient metastore;
  private final Table table;
  private final short bufferSize;
  private Map<String, Partition> buffer;

  public BufferedPartitionFetcher(IMetaStoreClient metastore, Table table, short bufferSize) {

    try {
      LOG.debug("Fetching all partition names.");
      partitionNames = metastore.listPartitionNames(table.getDbName(), table.getTableName(), NO_LIMIT);
      LOG.debug("Fetched {} partition names for table {}.", partitionNames.size(), Warehouse.getQualifiedName(table));
    } catch (TException e) {
      throw new RuntimeException("Unable to fetch partition names of table " + Warehouse.getQualifiedName(table), e);
    }

    this.table = table;
    this.metastore = metastore;
    this.bufferSize = bufferSize;
    buffer = Collections.emptyMap();
  }

  @Override
  public Partition fetch(String partitionName) {
    int partitionPosition = partitionNames.indexOf(partitionName);
    if (partitionPosition < 0) {
      throw new PartitionNotFoundException("Unknown partition " + partitionName);
    }

    if (!buffer.containsKey(partitionName)) {
      bufferPartitions(partitionPosition);
    }

    return buffer.get(partitionName);
  }

  @VisibleForTesting
  void bufferPartitions(int firstPartition) {
    int totalPartitionsToLoad = Math.min(partitionNames.size(), firstPartition + bufferSize);
    List<String> partitionsToLoad = partitionNames.subList(firstPartition, totalPartitionsToLoad);

    try {
      LOG.debug("Fetching {} partitions.", totalPartitionsToLoad);
      List<Partition> partitions = metastore.getPartitionsByNames(table.getDbName(), table.getTableName(),
          partitionsToLoad);
      LOG.debug("Fetched {} partitions for table {}.", partitions.size(), Warehouse.getQualifiedName(table));

      buffer = new HashMap<>(partitions.size());
      for (Partition partition : partitions) {
        buffer.put(Warehouse.makePartName(table.getPartitionKeys(), partition.getValues()), partition);
      }
    } catch (TException e) {
      throw new RuntimeException("Unable to fetch partitions of table " + Warehouse.getQualifiedName(table), e);
    }
  }

}
