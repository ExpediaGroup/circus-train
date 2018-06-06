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
package com.hotels.bdp.circustrain.tool.vacuum;

import static com.hotels.hcommon.hive.metastore.util.LocationUtils.locationAsPath;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hotels.hcommon.hive.metastore.iterator.PartitionIterator;

class PartitionedTablePathResolver implements TablePathResolver {

  private static final Logger LOG = LoggerFactory.getLogger(PartitionedTablePathResolver.class);

  private final Path tableBaseLocation;
  private final Path globPath;
  private final IMetaStoreClient metastore;
  private final Table table;

  PartitionedTablePathResolver(IMetaStoreClient metastore, Table table)
      throws NoSuchObjectException, MetaException, TException {
    this.metastore = metastore;
    this.table = table;
    LOG.debug("Table '{}' is partitioned", Warehouse.getQualifiedName(table));
    tableBaseLocation = locationAsPath(table);
    List<Partition> onePartition = metastore.listPartitions(table.getDbName(), table.getTableName(), (short) 1);
    if (onePartition.isEmpty()) {
      LOG.warn("Table '{}' has no partitions, perhaps you can simply delete: {}.", Warehouse.getQualifiedName(table),
          tableBaseLocation);
      throw new ConfigurationException();
    }
    Path partitionLocation = locationAsPath(onePartition.get(0));
    int branches = partitionLocation.depth() - tableBaseLocation.depth();
    String globSuffix = StringUtils.repeat("*", "/", branches);
    globPath = new Path(tableBaseLocation, globSuffix);
  }

  @Override
  public Path getGlobPath() {
    return globPath;
  }

  @Override
  public Path getTableBaseLocation() {
    return tableBaseLocation;
  }

  @Override
  public Set<Path> getMetastorePaths(short batchSize, int expectedPathCount)
    throws NoSuchObjectException, MetaException, TException {
    Set<Path> metastorePaths = new HashSet<>(expectedPathCount);
    PartitionIterator partitionIterator = new PartitionIterator(metastore, table, batchSize);
    while (partitionIterator.hasNext()) {
      Partition partition = partitionIterator.next();
      Path location = PathUtils.normalise(locationAsPath(partition));
      if (!location.toString().toLowerCase().startsWith(tableBaseLocation.toString().toLowerCase())) {
        LOG.error("Check your configuration: '{}' does not appear to be part of '{}'.", location, tableBaseLocation);
        throw new ConfigurationException();
      }
      metastorePaths.add(location);
    }
    return metastorePaths;
  }

}
