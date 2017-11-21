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
package com.hotels.bdp.circustrain.tool.filter;

import static java.lang.System.out;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ComparisonChain;

import com.hotels.bdp.circustrain.api.CircusTrainException;
import com.hotels.bdp.circustrain.core.HiveEndpoint;
import com.hotels.bdp.circustrain.core.PartitionPredicate;
import com.hotels.bdp.circustrain.core.PartitionsAndStatistics;

class FilterGeneratorImpl implements FilterGenerator {

  private static final Logger LOG = LoggerFactory.getLogger(FilterGeneratorImpl.class);

  private static final Comparator<Partition> PARTITION_COMPARATOR = new Comparator<Partition>() {
    @Override
    public int compare(Partition o1, Partition o2) {
      return ComparisonChain.start().compare(o1.getValues().toString(), o2.getValues().toString()).result();
    }
  };

  private final HiveEndpoint source;
  private final String partitionFilter;
  private final short partitionLimit;
  private final Table sourceTable;

  private final PartitionPredicate partitionPredicate;

  FilterGeneratorImpl(
      HiveEndpoint source,
      Table sourceTable,
      String partitionFilter,
      PartitionPredicate partitionPredicate) {
    this.source = source;
    this.sourceTable = sourceTable;
    this.partitionFilter = partitionFilter;
    partitionLimit = partitionPredicate.getPartitionPredicateLimit();
    this.partitionPredicate = partitionPredicate;
  }

  @Override
  public void run() throws CircusTrainException {
    out.println(String.format("Source catalog:        %s", source.getName()));
    out.println(String.format("Source MetaStore URIs: %s", source.getMetaStoreUris()));
    out.println(String.format("Source table:          %s", Warehouse.getQualifiedName(sourceTable)));
    out.println(String.format("Partition expression:  %s", partitionFilter));

    String parsedPartitionFilter = partitionPredicate.getPartitionPredicate();
    if (!Objects.equals(partitionFilter, parsedPartitionFilter)) {
      LOG.info("Evaluated expression to: {}", parsedPartitionFilter);
    }
    try {
      LOG.info("Executing filter with limit {} on: {}:{} ({})", partitionLimit, source.getName(),
          Warehouse.getQualifiedName(sourceTable), source.getMetaStoreUris());
      PartitionsAndStatistics partitions = source.getPartitions(sourceTable, parsedPartitionFilter, partitionLimit);
      LOG.info("Retrieved {} partition(s):", partitions.getPartitions().size());
      SortedSet<Partition> sorted = new TreeSet<>(PARTITION_COMPARATOR);
      sorted.addAll(partitions.getPartitions());
      List<List<String>> vals = new ArrayList<>();
      for (Partition partition : sorted) {
        vals.add(partition.getValues());
        LOG.info("{}", partition.getValues());
      }
      out.println(String.format("Partition filter:      %s", parsedPartitionFilter));
      out.println(String.format("Partition limit:       %s", partitionLimit));
      out.println(String.format("Partition(s) fetched:  %s", vals));
    } catch (TException e) {
      throw new CircusTrainException("Could not fetch partitions for filter: '" + parsedPartitionFilter + "'.", e);
    }
  }
}
