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
package com.hotels.bdp.circustrain.core;

import javax.annotation.Nonnull;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Strings;

import com.hotels.bdp.circustrain.api.CircusTrainException;
import com.hotels.bdp.circustrain.api.metastore.CloseableMetaStoreClient;
import com.hotels.bdp.circustrain.comparator.ComparatorRegistry;
import com.hotels.bdp.circustrain.comparator.api.ComparatorType;
import com.hotels.bdp.circustrain.comparator.hive.HiveDifferences;
import com.hotels.bdp.circustrain.comparator.listener.PartitionSpecCreatingDiffListener;
import com.hotels.bdp.circustrain.core.conf.TableReplication;
import com.hotels.bdp.circustrain.hive.fetcher.BufferedPartitionFetcher;
import com.hotels.bdp.circustrain.hive.fetcher.PartitionFetcher;
import com.hotels.bdp.circustrain.hive.iterator.PartitionIterator;

public class DiffGeneratedPartitionPredicate implements PartitionPredicate {

  private final TableReplication tableReplication;
  private final HiveEndpoint source;
  private final HiveEndpoint replica;

  private final Function<Path, String> checksumFunction;
  private String partitionPredicate;
  private boolean generated = false;

  public DiffGeneratedPartitionPredicate(
      @Nonnull HiveEndpoint source,
      @Nonnull HiveEndpoint replica,
      TableReplication tableReplication,
      Function<Path, String> checksumFunction) {
    this.source = source;
    this.replica = replica;
    this.tableReplication = tableReplication;
    this.checksumFunction = checksumFunction;
  }

  private String generate() {
    try (CloseableMetaStoreClient sourceMetastore = source.getMetaStoreClientSupplier().get()) {
      try (CloseableMetaStoreClient replicaMetastore = replica.getMetaStoreClientSupplier().get()) {
        Table sourceTable = source.getTableAndStatistics(tableReplication).getTable();
        PartitionIterator partitionIterator = new PartitionIterator(sourceMetastore, sourceTable,
            tableReplication.getPartitionIteratorBatchSize());
        Optional<Table> replicaTable = getReplicaTable(tableReplication);
        Optional<? extends PartitionFetcher> replicaPartitionFetcher = Optional.absent();
        if (replicaTable.isPresent()) {
          replicaPartitionFetcher = Optional.of(new BufferedPartitionFetcher(replicaMetastore, replicaTable.get(),
              tableReplication.getPartitionFetcherBufferSize()));
        }
        PartitionSpecCreatingDiffListener diffListener = new PartitionSpecCreatingDiffListener(source.getHiveConf());
        HiveDifferences diffs = HiveDifferences
            .builder(diffListener)
            .checksumFunction(checksumFunction)
            .comparatorRegistry(comparatorRegistry())
            .source(source.getHiveConf(), sourceTable, partitionIterator)
            .replica(replicaTable, replicaPartitionFetcher)
            .build();
        diffs.run();
        return diffListener.getPartitionSpecFilter();
      } catch (TException e) {
        throw new CircusTrainException("Cannot auto generate partition filter, error: ", e);
      }
    }
  }

  private Optional<Table> getReplicaTable(TableReplication tableReplication) {
    try {
      return Optional.of(replica.getTableAndStatistics(tableReplication).getTable());
    } catch (CircusTrainException e) {
      // replica doesn't exist
      return Optional.absent();
    }
  }

  private ComparatorRegistry comparatorRegistry() {
    return new ComparatorRegistry(ComparatorType.SHORT_CIRCUIT);
  }

  @Override
  public String getPartitionPredicate() {
    if (!generated) {
      partitionPredicate = generate();
      generated = true;
    }
    return partitionPredicate;

  }

  @Override
  public short getPartitionPredicateLimit() {
    if (Strings.isNullOrEmpty(getPartitionPredicate())) {
      // if the generated partition predicate has no meaningful value cut the limit to 0 as no partitions should be
      // fetched.
      return 0;
    }
    Short partitionLimit = tableReplication.getSourceTable().getPartitionLimit();
    return partitionLimit == null ? -1 : partitionLimit;
  }

}
