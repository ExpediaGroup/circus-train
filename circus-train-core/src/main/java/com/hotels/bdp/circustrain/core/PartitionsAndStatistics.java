/**
 * Copyright (C) 2016-2019 Expedia, Inc.
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsDesc;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PartitionsAndStatistics {

  private static final Logger LOG = LoggerFactory.getLogger(PartitionsAndStatistics.class);

  private final Map<Partition, ColumnStatistics> statisticsByPartition;
  private final List<String> partitionNames = new ArrayList<>();
  private List<FieldSchema> partitionKeys;

  public PartitionsAndStatistics(
      List<FieldSchema> partitionKeys,
      List<Partition> partitions,
      Map<String, List<ColumnStatisticsObj>> statisticsByPartitionName) {
    this(partitionKeys, createStatisticsByPartitionMap(partitionKeys, partitions, statisticsByPartitionName));
  }

  public PartitionsAndStatistics(
      List<FieldSchema> partitionKeys,
      Map<Partition, ColumnStatistics> statisticsByPartition) {
    this.partitionKeys = partitionKeys;
    this.statisticsByPartition = statisticsByPartition;
    for (Partition partition : statisticsByPartition.keySet()) {
      String partitionName = getPartitionName(partitionKeys, partition);
      partitionNames.add(partitionName);
    }
  }

  private static Map<Partition, ColumnStatistics> createStatisticsByPartitionMap(
      List<FieldSchema> partitionKeys,
      List<Partition> partitions,
      Map<String, List<ColumnStatisticsObj>> statisticsByPartitionName) {
    int entryCount = 0;
    // LinkedHashMap so order is preserved, nice for tests mostly
    Map<Partition, ColumnStatistics> result = new LinkedHashMap<>(partitions.size());
    for (Partition partition : partitions) {
      if (partition == null) {
        throw new IllegalArgumentException("partition == null");
      }
      String partitionName = getPartitionName(partitionKeys, partition);
      ColumnStatistics statistics = null;
      List<ColumnStatisticsObj> statisticsObj = statisticsByPartitionName.get(partitionName);
      if (statisticsObj != null && !statisticsObj.isEmpty()) {
        entryCount += statisticsObj.size();
        ColumnStatisticsDesc statsDesc = new ColumnStatisticsDesc(false, partition.getDbName(),
            partition.getTableName());
        statsDesc.setPartName(partitionName);
        statistics = new ColumnStatistics(statsDesc, statisticsObj);
      }
      result.put(partition, statistics);
    }
    LOG.debug("Indexed {} column stats entries for {} partitions.", entryCount, statisticsByPartitionName.size());
    return result;
  }

  private static String getPartitionName(List<FieldSchema> partitionKeys, Partition partition) {
    try {
      return Warehouse.makePartName(partitionKeys, partition.getValues());
    } catch (MetaException e) {
      throw new RuntimeException(e);
    }
  }

  public List<Partition> getPartitions() {
    return Collections.unmodifiableList(new ArrayList<>(statisticsByPartition.keySet()));
  }
  
  public List<FieldSchema> getPartitionKeys() {
    return Collections.unmodifiableList(partitionKeys);
  }

  public ColumnStatistics getStatisticsForPartition(Partition partition) {
    if (partition == null) {
      throw new IllegalArgumentException("partition == null");
    }
    return statisticsByPartition.get(partition);
  }

  /**
   * @return list of partition names example: [key1=a/key2=b, key1=c/key2=d]
   */
  public List<String> getPartitionNames() {
    return Collections.unmodifiableList(partitionNames);
  }

}
