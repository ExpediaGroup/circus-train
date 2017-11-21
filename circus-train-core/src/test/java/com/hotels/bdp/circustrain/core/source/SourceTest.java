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
package com.hotels.bdp.circustrain.core.source;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.TableType;
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

import com.hotels.bdp.circustrain.api.SourceLocationManager;
import com.hotels.bdp.circustrain.api.copier.CopierOptions;
import com.hotels.bdp.circustrain.api.event.SourceCatalogListener;
import com.hotels.bdp.circustrain.api.metastore.CloseableMetaStoreClient;
import com.hotels.bdp.circustrain.core.PartitionsAndStatistics;
import com.hotels.bdp.circustrain.core.TableAndStatistics;
import com.hotels.bdp.circustrain.core.conf.SourceCatalog;

@RunWith(MockitoJUnitRunner.class)
public class SourceTest {

  private static final String NAME = "name";
  private static final String TABLE_LOCATION = "tableLocation";
  private static final String TABLE_BASE_PATH = "tableBasePath";
  private static final String EVENT_ID = "eventId";
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
  private static final String PARTITION_NAME = "c=one/d=two";
  private static final List<String> PARTITION_NAMES = Arrays.asList(PARTITION_NAME);
  private static final List<FieldSchema> FIELDS = Arrays.asList(FIELD_A, FIELD_B);
  private static final List<FieldSchema> PARTITIONS = Arrays.asList(FIELD_C, FIELD_D);

  private @Mock SourceCatalog sourceCatalog;
  private @Mock Supplier<CloseableMetaStoreClient> metaStoreClientSupplier;
  private @Mock CloseableMetaStoreClient metaStoreClient;
  private @Mock SourceCatalogListener sourceCatalogListener;

  private Partition partition;
  private List<Partition> partitions;
  private final Table table = new Table();
  private final HiveConf hiveConf = new HiveConf(new Configuration(false), getClass());
  private Source source;
  private ColumnStatistics columnStatistics;
  private List<ColumnStatisticsObj> columnStatisticsObjs;
  private Map<String, List<ColumnStatisticsObj>> partitionStatsMap;
  private ColumnStatistics partitionColumnStatistics;
  private final Map<String, Object> copierOptions = Collections.<String, Object> emptyMap();

  @Before
  public void setupTable() {
    when(metaStoreClientSupplier.get()).thenReturn(metaStoreClient);
    when(sourceCatalog.getName()).thenReturn(NAME);
    when(sourceCatalog.isDisableSnapshots()).thenReturn(true);

    table.setDbName(DATABASE);
    table.setTableName(TABLE);
    StorageDescriptor sd = new StorageDescriptor();
    sd.setLocation(TABLE_LOCATION);
    sd.setCols(FIELDS);
    table.setSd(sd);
    table.setPartitionKeys(PARTITIONS);

    partition = new Partition();
    partition.setDbName(DATABASE);
    partition.setTableName(TABLE);
    partition.setValues(Arrays.asList("one", "two"));
    sd = new StorageDescriptor();
    sd.setLocation(TABLE_LOCATION + "/" + PARTITION_NAME);
    sd.setCols(FIELDS);
    partition.setSd(sd);
    partitions = Arrays.asList(partition);

    source = new Source(sourceCatalog, hiveConf, metaStoreClientSupplier, sourceCatalogListener, true, null);

    ColumnStatisticsObj columnStatisticsObj1 = new ColumnStatisticsObj(COLUMN_A, "string",
        new ColumnStatisticsData(_Fields.LONG_STATS, new LongColumnStatsData(0, 1)));
    ColumnStatisticsObj columnStatisticsObj2 = new ColumnStatisticsObj(COLUMN_B, "string",
        new ColumnStatisticsData(_Fields.LONG_STATS, new LongColumnStatsData(1, 2)));
    columnStatisticsObjs = Arrays.asList(columnStatisticsObj1, columnStatisticsObj2);
    ColumnStatisticsDesc statsDesc = new ColumnStatisticsDesc(true, DATABASE, TABLE);
    columnStatistics = new ColumnStatistics(statsDesc, columnStatisticsObjs);

    ColumnStatisticsDesc partitionStatsDesc = new ColumnStatisticsDesc(false, DATABASE, TABLE);
    partitionStatsDesc.setPartName(PARTITION_NAME);
    partitionColumnStatistics = new ColumnStatistics(partitionStatsDesc, columnStatisticsObjs);
    partitionStatsMap = new HashMap<>();
    partitionStatsMap.put(PARTITION_NAME, columnStatisticsObjs);
  }

  @Test
  public void getName() throws Exception {
    assertThat(source.getName(), is(NAME));
  }

  @Test
  public void getTable() throws Exception {
    when(metaStoreClient.getTable(DATABASE, TABLE)).thenReturn(table);
    when(metaStoreClient.getTableColumnStatistics(DATABASE, TABLE, COLUMN_NAMES)).thenReturn(columnStatisticsObjs);

    TableAndStatistics sourceTable = source.getTableAndStatistics(DATABASE, TABLE);
    assertThat(sourceTable.getTable(), is(table));
    assertThat(sourceTable.getStatistics(), is(columnStatistics));
  }

  @Test
  public void getTableNoStats() throws Exception {
    when(metaStoreClient.getTable(DATABASE, TABLE)).thenReturn(table);
    when(metaStoreClient.getTableColumnStatistics(DATABASE, TABLE, COLUMN_NAMES))
        .thenReturn(Collections.<ColumnStatisticsObj> emptyList());

    TableAndStatistics sourceTable = source.getTableAndStatistics(DATABASE, TABLE);
    assertThat(sourceTable.getTable(), is(table));
    assertThat(sourceTable.getStatistics(), is(nullValue()));
  }

  @Test
  public void getPartitions() throws Exception {
    when(metaStoreClient.listPartitionsByFilter(DATABASE, TABLE, PARTITION_PREDICATE, (short) MAX_PARTITIONS))
        .thenReturn(partitions);
    when(metaStoreClient.getPartitionColumnStatistics(DATABASE, TABLE, PARTITION_NAMES, COLUMN_NAMES))
        .thenReturn(partitionStatsMap);

    PartitionsAndStatistics partitionsAndStatistics = source.getPartitions(table, PARTITION_PREDICATE, MAX_PARTITIONS);
    assertThat(partitionsAndStatistics.getPartitions(), is(partitions));
    assertThat(partitionsAndStatistics.getStatisticsForPartition(partition), is(partitionColumnStatistics));
  }

  @Test
  public void getPartitionsNoStats() throws Exception {
    when(metaStoreClient.listPartitionsByFilter(DATABASE, TABLE, PARTITION_PREDICATE, (short) MAX_PARTITIONS))
        .thenReturn(partitions);
    when(metaStoreClient.getPartitionColumnStatistics(DATABASE, TABLE, PARTITION_NAMES, COLUMN_NAMES))
        .thenReturn(Collections.<String, List<ColumnStatisticsObj>> emptyMap());

    PartitionsAndStatistics partitionsAndStatistics = source.getPartitions(table, PARTITION_PREDICATE, MAX_PARTITIONS);
    assertThat(partitionsAndStatistics.getPartitions(), is(partitions));
    assertThat(partitionsAndStatistics.getStatisticsForPartition(partition), is(nullValue()));
  }

  @Test
  public void getHiveConf() throws Exception {
    assertThat(source.getHiveConf(), is(hiveConf));
  }

  @Test
  public void getLocationManagerForTable() throws Exception {
    SourceLocationManager locationManager = source.getLocationManager(table, EVENT_ID);
    assertThat(locationManager.getTableLocation(), is(new Path(TABLE_LOCATION)));
  }

  @Test
  public void getLocationManagerForPartitionedTable() throws Exception {
    StorageDescriptor sd = new StorageDescriptor();
    sd.setLocation(TABLE_LOCATION + "/partition");
    partition.setSd(sd);

    SourceLocationManager locationManager = source.getLocationManager(table, partitions, EVENT_ID, copierOptions);
    assertThat(locationManager.getTableLocation(), is(new Path(TABLE_LOCATION)));
  }

  @Test
  public void getLocationManagerForPartitionedTableWithBaseOverride() throws Exception {
    StorageDescriptor sd = new StorageDescriptor();
    sd.setLocation(TABLE_BASE_PATH + "/partition");
    partition.setSd(sd);

    Source source = new Source(sourceCatalog, hiveConf, metaStoreClientSupplier, sourceCatalogListener, true,
        TABLE_BASE_PATH);
    SourceLocationManager locationManager = source.getLocationManager(table, partitions, EVENT_ID, copierOptions);
    assertThat(locationManager.getTableLocation(), is(new Path(TABLE_BASE_PATH)));
  }

  @Test
  public void getFilteringLocationManagerForPartitionedTableFlagTrue() throws Exception {
    Map<String, Object> copierOptions = new HashMap<>();
    copierOptions.put(CopierOptions.IGNORE_MISSING_PARTITION_FOLDER_ERRORS, "true");
    SourceLocationManager locationManager = source.getLocationManager(table, partitions, EVENT_ID, copierOptions);
    assertThat(locationManager, instanceOf(FilterMissingPartitionsLocationManager.class));
  }

  @Test
  public void getNormalLocationManagerForPartitionedTableFlagFalse() throws Exception {
    Map<String, Object> copierOptions = new HashMap<>();
    copierOptions.put(CopierOptions.IGNORE_MISSING_PARTITION_FOLDER_ERRORS, "false");
    SourceLocationManager locationManager = source.getLocationManager(table, partitions, EVENT_ID, copierOptions);
    assertThat(locationManager, instanceOf(HdfsSnapshotLocationManager.class));
  }

  @Test
  public void getNormalLocationManagerForPartitionedTableFlagNull() throws Exception {
    Map<String, Object> copierOptions = new HashMap<>();
    copierOptions.put(CopierOptions.IGNORE_MISSING_PARTITION_FOLDER_ERRORS, null);
    SourceLocationManager locationManager = source.getLocationManager(table, partitions, EVENT_ID, copierOptions);
    assertThat(locationManager, instanceOf(HdfsSnapshotLocationManager.class));
  }

  @Test
  public void getViewLocationManagerForTable() throws Exception {
    table.setTableType(TableType.VIRTUAL_VIEW.name());
    SourceLocationManager locationManager = source.getLocationManager(table, EVENT_ID);
    assertThat(locationManager, is(instanceOf(ViewLocationManager.class)));
  }

  @Test
  public void getVoidLocationManagerForPartitionedTable() throws Exception {
    table.setTableType(TableType.VIRTUAL_VIEW.name());
    partition.getSd().setLocation(null);
    SourceLocationManager locationManager = source.getLocationManager(table, partitions, EVENT_ID, copierOptions);
    assertThat(locationManager, is(instanceOf(ViewLocationManager.class)));
  }

  @Test
  public void overrideUnpartitionedTableLocation() throws Exception {
    source = new Source(sourceCatalog, hiveConf, metaStoreClientSupplier, sourceCatalogListener, true,
        "file:///foo/bar");
    SourceLocationManager locationManager = source.getLocationManager(table, EVENT_ID);
    assertThat(locationManager.getTableLocation(), is(new Path("file:///foo/bar")));
  }

  @Test
  public void overridePartitionedTableLocationWithConfigLocation() throws Exception {
    source = new Source(sourceCatalog, hiveConf, metaStoreClientSupplier, sourceCatalogListener, true,
        "file:///abc/xyz");
    SourceLocationManager locationManager = source.getLocationManager(table, partitions, EVENT_ID, copierOptions);
    assertThat(locationManager.getTableLocation(), is(new Path("file:///abc/xyz")));
  }

}
