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
package com.hotels.bdp.circustrain.hive.iterator;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Iterates over partitions, lazily loading in batches where possible. */
public class PartitionIterator implements Iterator<Partition> {

  private static final Logger LOG = LoggerFactory.getLogger(PartitionIterator.class);

  private static final short NO_LIMIT = (short) -1;

  private static List<String> loadPartitionNames(IMetaStoreClient metastore, Table table)
    throws MetaException, TException {
    // partition names: (key=value/)*(key=value)
    LOG.debug("Fetching all partition names.");
    List<String> names = metastore.listPartitionNames(table.getDbName(), table.getTableName(), NO_LIMIT);
    LOG.debug("Fetched {} partition names for table {}.", names.size(), Warehouse.getQualifiedName(table));
    return names;
  }

  private final Iterator<List<String>> partitionNames;
  private final IMetaStoreClient metastore;
  private final Table table;
  private int count;

  private Iterator<Partition> batch;

  public PartitionIterator(IMetaStoreClient metastore, Table table, short batchSize) throws MetaException, TException {
    this(metastore, table, new BatchResolver(loadPartitionNames(metastore, table), batchSize));
  }

  public PartitionIterator(IMetaStoreClient metastore, Table table, short batchSize, List<String> partitionNames)
      throws MetaException, TException {
    this(metastore, table, new BatchResolver(partitionNames, batchSize));
  }

  PartitionIterator(IMetaStoreClient metastore, Table table, BatchResolver batchResolver)
      throws MetaException, TException {
    partitionNames = batchResolver.resolve().iterator();
    batch = Collections.<Partition> emptyList().iterator();
    this.table = table;
    this.metastore = metastore;
  }

  @Override
  public boolean hasNext() {
    if (batch.hasNext()) {
      return true;
    }
    if (partitionNames.hasNext()) {
      List<String> names = partitionNames.next();
      try {
        List<Partition> partitions = metastore.getPartitionsByNames(table.getDbName(), table.getTableName(), names);
        count += partitions.size();
        LOG.debug("Retrieved {} partitions, total: {}.", partitions.size(), count);
        batch = partitions.iterator();
      } catch (TException e) {
        throw new RuntimeException(e);
      }
    }
    return batch.hasNext();
  }

  @Override
  public Partition next() {
    return batch.next();
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException("Remove is not supported");
  }

}
