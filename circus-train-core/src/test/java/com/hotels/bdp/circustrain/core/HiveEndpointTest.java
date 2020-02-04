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

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData._Fields;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsDesc;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.LongColumnStatsData;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.base.Supplier;

import com.hotels.bdp.circustrain.api.conf.TableReplication;
import com.hotels.hcommon.hive.metastore.client.api.CloseableMetaStoreClient;

@RunWith(MockitoJUnitRunner.class)
public class HiveEndpointTest {

  private static final String NAME = "name";
  private static final String TABLE_LOCATION = "tableLocation";
  private static final int MAX_PARTITIONS = 1;
  private static final String PARTITION_PREDICATE = "partitionPredicate";
  private static final String TABLE = "table";
  private static final String DATABASE = "database";
  private static final String COLUMN_A = "a";
  private static final String COLUMN_B = "b";
  private static final String COLUMN_C = "c";
  private static final String COLUMN_D = "d";
  private static final FieldSchema FIELD_A = new FieldSchema(COLUMN_A, "string", null);
  private static final FieldSchema FIELD_B = new FieldSchema(COLUMN_B, "string", null);
  private static final FieldSchema FIELD_C = new FieldSchema(COLUMN_C, "string", null);
  private static final FieldSchema FIELD_D = new FieldSchema(COLUMN_D, "string", null);
  private static final List<String> COLUMN_NAMES = Arrays.asList(COLUMN_A, COLUMN_B);
  private static final String PARTITION_ONE_TWO = "c=one/d=two";
  private static final String PARTITION_THREE_FOUR = "c=three/d=four";
  private static final List<String> PARTITION_NAMES = Arrays.asList(PARTITION_ONE_TWO);
  private static final List<FieldSchema> FIELDS = Arrays.asList(FIELD_A, FIELD_B);
  private static final List<FieldSchema> PARTITIONS = Arrays.asList(FIELD_C, FIELD_D);

  private @Mock Supplier<CloseableMetaStoreClient> metaStoreClientSupplier;
  private @Mock CloseableMetaStoreClient metaStoreClient;

  private Partition partitionOneTwo;
  private Partition partitionThreeFour;
  private List<Partition> partitions;
  private final Table table = new Table();
  private final HiveConf hiveConf = new HiveConf(new Configuration(false), getClass());
  private HiveEndpoint hiveEndpoint;
  private ColumnStatistics columnStatistics;
  private List<ColumnStatisticsObj> columnStatisticsObjs;
  private Map<String, List<ColumnStatisticsObj>> partitionStatsMap;
  private ColumnStatistics partitionColumnStatistics;

  @Before
  public void setupTable() {
    when(metaStoreClientSupplier.get()).thenReturn(metaStoreClient);

    table.setDbName(DATABASE);
    table.setTableName(TABLE);
    StorageDescriptor sd = new StorageDescriptor();
    sd.setLocation(TABLE_LOCATION);
    sd.setCols(FIELDS);
    table.setSd(sd);
    table.setPartitionKeys(PARTITIONS);

    partitionOneTwo = new Partition();
    partitionOneTwo.setDbName(DATABASE);
    partitionOneTwo.setTableName(TABLE);
    partitionOneTwo.setValues(Arrays.asList("one", "two"));
    sd = new StorageDescriptor();
    sd.setLocation(TABLE_LOCATION + "/" + PARTITION_ONE_TWO);
    sd.setCols(FIELDS);
    partitionOneTwo.setSd(sd);
    partitions = Arrays.asList(partitionOneTwo);

    partitionThreeFour = new Partition();
    partitionThreeFour.setDbName(DATABASE);
    partitionThreeFour.setTableName(TABLE);
    partitionThreeFour.setValues(Arrays.asList("three", "four"));
    sd = new StorageDescriptor();
    sd.setLocation(TABLE_LOCATION + "/" + PARTITION_THREE_FOUR);
    sd.setCols(FIELDS);
    partitionThreeFour.setSd(sd);
    partitions = Arrays.asList(partitionThreeFour);

    hiveEndpoint = new HiveEndpoint(NAME, hiveConf, metaStoreClientSupplier) {

      @Override
      public TableAndStatistics getTableAndStatistics(TableReplication tableReplication) {
        // We don't care for this in this test
        return null;
      }

    };

    partitionStatsMap = new HashMap<>();

    ColumnStatisticsObj columnStatisticsObj1 = new ColumnStatisticsObj(COLUMN_A, "string",
        new ColumnStatisticsData(_Fields.LONG_STATS, new LongColumnStatsData(0, 1)));
    ColumnStatisticsObj columnStatisticsObj2 = new ColumnStatisticsObj(COLUMN_B, "string",
        new ColumnStatisticsData(_Fields.LONG_STATS, new LongColumnStatsData(1, 2)));
    columnStatisticsObjs = Arrays.asList(columnStatisticsObj1, columnStatisticsObj2);
    ColumnStatisticsDesc statsDesc = new ColumnStatisticsDesc(true, DATABASE, TABLE);
    columnStatistics = new ColumnStatistics(statsDesc, columnStatisticsObjs);

    ColumnStatisticsDesc partitionStatsDescOneTwo = new ColumnStatisticsDesc(false, DATABASE, TABLE);
    partitionStatsDescOneTwo.setPartName(PARTITION_ONE_TWO);
    partitionColumnStatistics = new ColumnStatistics(partitionStatsDescOneTwo, columnStatisticsObjs);
    partitionStatsMap.put(PARTITION_ONE_TWO, columnStatisticsObjs);
  }

  @Test
  public void getName() throws Exception {
    assertThat(hiveEndpoint.getName(), is(NAME));
  }

  @Test
  public void getHiveConf() throws Exception {
    assertThat(hiveEndpoint.getHiveConf(), is(hiveConf));
  }

  @Test
  public void getTable() throws Exception {
    when(metaStoreClient.getTable(DATABASE, TABLE)).thenReturn(table);
    when(metaStoreClient.getTableColumnStatistics(DATABASE, TABLE, COLUMN_NAMES)).thenReturn(columnStatisticsObjs);

    TableAndStatistics sourceTable = hiveEndpoint.getTableAndStatistics(DATABASE, TABLE);
    assertThat(sourceTable.getTable(), is(table));
    assertThat(sourceTable.getStatistics(), is(columnStatistics));
  }

  @Test
  public void getTableNoStats() throws Exception {
    when(metaStoreClient.getTable(DATABASE, TABLE)).thenReturn(table);
    when(metaStoreClient.getTableColumnStatistics(DATABASE, TABLE, COLUMN_NAMES))
        .thenReturn(Collections.<ColumnStatisticsObj> emptyList());

    TableAndStatistics sourceTable = hiveEndpoint.getTableAndStatistics(DATABASE, TABLE);
    assertThat(sourceTable.getTable(), is(table));
    assertThat(sourceTable.getStatistics(), is(nullValue()));
  }

  @Test
  public void getPartitions() throws Exception {
    List<Partition> filteredPartitions = Arrays.asList(partitionOneTwo);
    when(metaStoreClient.listPartitionsByFilter(DATABASE, TABLE, PARTITION_PREDICATE, (short) MAX_PARTITIONS))
        .thenReturn(filteredPartitions);
    when(metaStoreClient.getPartitionColumnStatistics(DATABASE, TABLE, PARTITION_NAMES, COLUMN_NAMES))
        .thenReturn(partitionStatsMap);

    PartitionsAndStatistics partitionsAndStatistics = hiveEndpoint.getPartitions(table, PARTITION_PREDICATE,
        MAX_PARTITIONS);
    assertThat(partitionsAndStatistics.getPartitions(), is(filteredPartitions));
    assertThat(partitionsAndStatistics.getStatisticsForPartition(partitionOneTwo), is(partitionColumnStatistics));
  }

  @Test
  public void getPartitionsWithoutFilter() throws Exception {
    when(metaStoreClient.listPartitions(DATABASE, TABLE, (short) MAX_PARTITIONS)).thenReturn(partitions);
    when(metaStoreClient.getPartitionColumnStatistics(DATABASE, TABLE, PARTITION_NAMES, COLUMN_NAMES))
        .thenReturn(partitionStatsMap);

    PartitionsAndStatistics partitionsAndStatistics = hiveEndpoint.getPartitions(table, null, MAX_PARTITIONS);
    assertThat(partitionsAndStatistics.getPartitions(), is(partitions));
  }

  @Test
  public void getPartitionsNoStats() throws Exception {
    List<Partition> filteredPartitions = Arrays.asList(partitionOneTwo);
    when(metaStoreClient.listPartitionsByFilter(DATABASE, TABLE, PARTITION_PREDICATE, (short) MAX_PARTITIONS))
        .thenReturn(filteredPartitions);
    when(metaStoreClient.getPartitionColumnStatistics(DATABASE, TABLE, PARTITION_NAMES, COLUMN_NAMES))
        .thenReturn(Collections.<String, List<ColumnStatisticsObj>> emptyMap());

    PartitionsAndStatistics partitionsAndStatistics = hiveEndpoint.getPartitions(table, PARTITION_PREDICATE,
        MAX_PARTITIONS);
    assertThat(partitionsAndStatistics.getPartitions(), is(filteredPartitions));
    assertThat(partitionsAndStatistics.getStatisticsForPartition(partitionOneTwo), is(nullValue()));
  }

}
