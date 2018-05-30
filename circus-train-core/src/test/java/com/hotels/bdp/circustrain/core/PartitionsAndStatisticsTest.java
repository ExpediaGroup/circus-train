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
package com.hotels.bdp.circustrain.core;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

import static com.hotels.bdp.circustrain.core.HiveEntityFactory.newFieldSchema;
import static com.hotels.bdp.circustrain.core.HiveEntityFactory.newPartition;
import static com.hotels.bdp.circustrain.core.HiveEntityFactory.newStorageDescriptor;
import static com.hotels.bdp.circustrain.core.HiveEntityFactory.newTable;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsDesc;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

public class PartitionsAndStatisticsTest {

  private Map<String, List<ColumnStatisticsObj>> statisticsPerPartitionName;
  private List<ColumnStatisticsObj> columnStats;

  @Before
  public void setUp() {
    statisticsPerPartitionName = new HashMap<>();
    columnStats = Lists.newArrayList(new ColumnStatisticsObj());
  }

  @Test
  public void typical() throws Exception {
    List<FieldSchema> partitionKeys = Lists.newArrayList(newFieldSchema("a"), newFieldSchema("c"));
    Table table = newTable("t1", "db1", partitionKeys, newStorageDescriptor(new File("bla"), "col1"));
    List<Partition> partitions = Lists.newArrayList(newPartition(table, "b", "d"));
    statisticsPerPartitionName.put("a=b/c=d", columnStats);

    PartitionsAndStatistics partitionsAndStatistics = new PartitionsAndStatistics(partitionKeys, partitions,
        statisticsPerPartitionName);
    List<String> expectedName = Lists.newArrayList("a=b/c=d");

    assertThat(partitionsAndStatistics.getPartitionNames(), is(expectedName));
    assertThat(partitionsAndStatistics.getPartitions(), is(partitions));
    assertThat(partitionsAndStatistics.getPartitionKeys(), is(partitionKeys));
    ColumnStatisticsDesc statsDesc = new ColumnStatisticsDesc(false, "db1", "t1");
    statsDesc.setPartName("a=b/c=d");
    ColumnStatistics expectedStats = new ColumnStatistics(statsDesc, columnStats);
    assertThat(partitionsAndStatistics.getStatisticsForPartition(partitions.get(0)), is(expectedStats));
  }

  @Test
  public void emptyListOfPartitions() throws Exception {
    List<FieldSchema> partitionKeys = Lists.newArrayList(newFieldSchema("a"));
    List<Partition> partitions = Lists.newArrayList();

    PartitionsAndStatistics partitionsAndStatistics = new PartitionsAndStatistics(partitionKeys, partitions,
        statisticsPerPartitionName);

    assertThat(partitionsAndStatistics.getPartitionNames(), is(empty()));
    assertThat(partitionsAndStatistics.getPartitions(), is(empty()));
    assertThat(partitionsAndStatistics.getPartitionKeys(), is(partitionKeys));
  }

  @Test(expected = IllegalArgumentException.class)
  public void getStatisticsForPartitionNullArgument() {
    List<FieldSchema> partitionKeys = Lists.newArrayList(newFieldSchema("a"));
    List<Partition> partitions = Lists.newArrayList();
    PartitionsAndStatistics partitionsAndStatistics = new PartitionsAndStatistics(partitionKeys, partitions,
        statisticsPerPartitionName);

    partitionsAndStatistics.getStatisticsForPartition(null);
  }

  @Test
  public void getStatisticsForPartitionReturnsNullIfNotPresent() throws Exception {
    List<FieldSchema> partitionKeys = Lists.newArrayList(newFieldSchema("a"));
    Table table = newTable("t1", "db1", partitionKeys, newStorageDescriptor(new File("bla"), "col1"));
    List<Partition> partitions = Lists.newArrayList(newPartition(table, "b"));

    PartitionsAndStatistics partitionsAndStatistics = new PartitionsAndStatistics(partitionKeys, partitions,
        statisticsPerPartitionName);

    assertNull(partitionsAndStatistics.getStatisticsForPartition(partitions.get(0)));
  }

  @Test
  public void getStatisticsForPartitionReturnsNullIfEmptyStats() throws Exception {
    List<FieldSchema> partitionKeys = Lists.newArrayList(newFieldSchema("a"));
    Table table = newTable("t1", "db1", partitionKeys, newStorageDescriptor(new File("bla"), "col1"));
    List<Partition> partitions = Lists.newArrayList(newPartition(table, "b"));
    statisticsPerPartitionName.put("a=b", Collections.<ColumnStatisticsObj> emptyList());

    PartitionsAndStatistics partitionsAndStatistics = new PartitionsAndStatistics(partitionKeys, partitions,
        statisticsPerPartitionName);

    assertNull(partitionsAndStatistics.getStatisticsForPartition(partitions.get(0)));
  }
}
