/**
 * Copyright (C) 2016-2019 Expedia Inc.
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
package com.hotels.bdp.circustrain.core.replica;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import static com.hotels.bdp.circustrain.api.conf.ReplicationMode.FULL;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsDesc;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.Before;
import org.junit.Test;

import com.hotels.bdp.circustrain.api.metadata.ColumnStatisticsTransformation;
import com.hotels.bdp.circustrain.api.metadata.PartitionTransformation;
import com.hotels.bdp.circustrain.api.metadata.TableTransformation;
import com.hotels.bdp.circustrain.core.TableAndStatistics;

public class ReplicaTableFactoryTest {

  private static final Path REPLICA_DATA_DESTINATION = new Path("hdfs://someserver/replicaDataDestination");
  private static final String EVENT_ID = "eventId";
  private static final String TABLE_LOCATION = "table_location";
  private static final String PARTITION_LOCATION = "table_location/partitionLocation";
  private static final String TABLE_NAME = "tableName";
  private static final String DB_NAME = "dbName";
  private static final String MAPPED_TABLE_NAME = "mapped_table";
  private static final String MAPPED_DB_NAME = "mapped_database";
  private static final String SOURCE_META_STORE_URIS = "sourceMetaStoreUris";
  private static final Path REPLICA_PARTITION_SUBPATH = new Path("replicaPartitionLocation");
  private static final String INPUT_FORMAT = "inputFormat";
  private static final String OUTPUT_FORMAT = "outputFormat";
  private static final TableTransformation TABLE_TRANSFORMATION = new TableTransformation() {
    @Override
    public Table transform(Table table) {
      table.getSd().setOutputFormat("newOutputFormat");
      return table;
    }
  };
  private static final PartitionTransformation PARTITION_TRANSFORMATION = new PartitionTransformation() {
    @Override
    public Partition transform(Partition partition) {
      partition.getSd().setInputFormat("newInputFormat");
      return partition;
    }
  };
  private static final ColumnStatisticsTransformation COLUMN_STATISTICS_TRANSFORMATION = new ColumnStatisticsTransformation() {
    @Override
    public ColumnStatistics transform(ColumnStatistics columnStatistics) {
      ColumnStatisticsDesc statsDesc = new ColumnStatisticsDesc(false, "new_db", "new_table");
      statsDesc.setPartName("part=newPart");
      columnStatistics.setStatsDesc(statsDesc);
      return columnStatistics;
    }
  };

  private Table sourceTable;
  private TableAndStatistics sourceTableAndStats;
  private Partition sourcePartition;
  private final ReplicaTableFactory factory = new ReplicaTableFactory(SOURCE_META_STORE_URIS,
      TableTransformation.IDENTITY, PartitionTransformation.IDENTITY, ColumnStatisticsTransformation.IDENTITY);

  @Before
  public void prepare() throws Exception {
    sourceTable = new Table();
    sourceTable.setDbName(DB_NAME);
    sourceTable.setTableName(TABLE_NAME);

    StorageDescriptor sd = new StorageDescriptor();
    sd.setInputFormat(INPUT_FORMAT);
    sd.setOutputFormat(OUTPUT_FORMAT);
    sd.setLocation(TABLE_LOCATION);
    sourceTable.setSd(sd);

    Map<String, String> parameters = new HashMap<>();
    parameters.put(StatsSetupConst.ROW_COUNT, "1");
    sourceTable.setParameters(parameters);

    sourceTableAndStats = new TableAndStatistics(sourceTable, null);

    sourcePartition = new Partition();
    sourcePartition.setDbName(DB_NAME);
    sourcePartition.setTableName(TABLE_NAME);

    sd = new StorageDescriptor();
    sd.setInputFormat(INPUT_FORMAT);
    sd.setOutputFormat(OUTPUT_FORMAT);
    sd.setLocation(PARTITION_LOCATION);
    sourcePartition.setSd(sd);

    sourcePartition.setParameters(new HashMap<>(parameters));
  }

  @Test
  public void newTable() {
    TableAndStatistics replicaAndStats = factory.newReplicaTable(EVENT_ID, sourceTableAndStats, DB_NAME, TABLE_NAME,
        REPLICA_DATA_DESTINATION, FULL);
    Table replica = replicaAndStats.getTable();

    assertThat(replica.getDbName(), is(sourceTable.getDbName()));
    assertThat(replica.getTableName(), is(sourceTable.getTableName()));
    assertThat(replica.getSd().getInputFormat(), is(INPUT_FORMAT));
    assertThat(replica.getSd().getOutputFormat(), is(OUTPUT_FORMAT));
    assertThat(replica.getSd().getLocation(), is(REPLICA_DATA_DESTINATION.toUri().toString()));
    assertThat(replica.getParameters().get("com.hotels.bdp.circustrain.source.table"), is(DB_NAME + "." + TABLE_NAME));
    assertThat(replica.getParameters().get("com.hotels.bdp.circustrain.source.metastore.uris"),
        is(SOURCE_META_STORE_URIS));
    assertThat(replica.getParameters().get("com.hotels.bdp.circustrain.source.location"), is(TABLE_LOCATION));
    assertThat(replica.getParameters().get("com.hotels.bdp.circustrain.replication.event"), is(EVENT_ID));
    assertThat(replica.getParameters().get("com.hotels.bdp.circustrain.last.replicated"), is(not(nullValue())));
    assertThat(replica.getParameters().get("com.hotels.bdp.circustrain.replication.mode"), is(FULL.name()));
    assertThat(replica.getParameters().get("DO_NOT_UPDATE_STATS"), is("true"));
    assertThat(replica.getParameters().get("STATS_GENERATED_VIA_STATS_TASK"), is("true"));
    assertThat(replica.getParameters().get("STATS_GENERATED"), is("true"));
    assertThat(replica.getParameters().get(StatsSetupConst.ROW_COUNT), is("1"));
    assertThat(replica.getTableType(), is(TableType.EXTERNAL_TABLE.name()));
    assertThat(replica.getParameters().get("EXTERNAL"), is("TRUE"));
    assertTrue(MetaStoreUtils.isExternalTable(replica));

    assertThat(replicaAndStats.getStatistics(), is(nullValue()));
  }

  @Test
  public void newTableWithTransformation() {
    ReplicaTableFactory factory = new ReplicaTableFactory(SOURCE_META_STORE_URIS, TABLE_TRANSFORMATION,
        PartitionTransformation.IDENTITY, ColumnStatisticsTransformation.IDENTITY);

    TableAndStatistics replicaAndStats = factory.newReplicaTable(EVENT_ID, sourceTableAndStats, DB_NAME, TABLE_NAME,
        REPLICA_DATA_DESTINATION, FULL);
    Table replica = replicaAndStats.getTable();

    assertThat(replica.getDbName(), is(sourceTable.getDbName()));
    assertThat(replica.getTableName(), is(sourceTable.getTableName()));
    assertThat(replica.getSd().getInputFormat(), is(INPUT_FORMAT));
    assertThat(replica.getSd().getOutputFormat(), is("newOutputFormat"));
    assertThat(replica.getSd().getLocation(), is(REPLICA_DATA_DESTINATION.toUri().toString()));
    assertThat(replica.getParameters().get("com.hotels.bdp.circustrain.source.table"), is(DB_NAME + "." + TABLE_NAME));
    assertThat(replica.getParameters().get("com.hotels.bdp.circustrain.source.metastore.uris"),
        is(SOURCE_META_STORE_URIS));
    assertThat(replica.getParameters().get("com.hotels.bdp.circustrain.source.location"), is(TABLE_LOCATION));
    assertThat(replica.getParameters().get("com.hotels.bdp.circustrain.replication.event"), is(EVENT_ID));
    assertThat(replica.getParameters().get("com.hotels.bdp.circustrain.last.replicated"), is(not(nullValue())));
    assertThat(replica.getParameters().get("DO_NOT_UPDATE_STATS"), is("true"));
    assertThat(replica.getParameters().get("STATS_GENERATED_VIA_STATS_TASK"), is("true"));
    assertThat(replica.getParameters().get("STATS_GENERATED"), is("true"));
    assertThat(replica.getParameters().get(StatsSetupConst.ROW_COUNT), is("1"));

    assertThat(replicaAndStats.getStatistics(), is(nullValue()));
  }

  @Test
  public void newTableEncodedLocation() {
    TableAndStatistics replicaAndStats = factory.newReplicaTable(EVENT_ID, sourceTableAndStats, DB_NAME, TABLE_NAME,
        new Path(REPLICA_DATA_DESTINATION, "%25"), FULL);
    Table replica = replicaAndStats.getTable();

    assertThat(replica.getSd().getLocation(), is("hdfs://someserver/replicaDataDestination/%25"));
  }

  @Test
  public void newPartition() {
    Path replicaPartitionPath = new Path(REPLICA_DATA_DESTINATION, REPLICA_PARTITION_SUBPATH);
    Partition replica = factory.newReplicaPartition(EVENT_ID, sourceTable, sourcePartition, DB_NAME, TABLE_NAME,
        replicaPartitionPath, FULL);

    assertThat(replica.getDbName(), is(sourceTable.getDbName()));
    assertThat(replica.getTableName(), is(sourceTable.getTableName()));
    assertThat(replica.getSd().getInputFormat(), is(INPUT_FORMAT));
    assertThat(replica.getSd().getOutputFormat(), is(OUTPUT_FORMAT));
    assertThat(replica.getSd().getLocation(), is(replicaPartitionPath.toUri().toString()));
    assertThat(replica.getParameters().get("com.hotels.bdp.circustrain.source.table"), is(DB_NAME + "." + TABLE_NAME));
    assertThat(replica.getParameters().get("com.hotels.bdp.circustrain.source.metastore.uris"),
        is(SOURCE_META_STORE_URIS));
    assertThat(replica.getParameters().get("com.hotels.bdp.circustrain.source.location"), is(PARTITION_LOCATION));
    assertThat(replica.getParameters().get("com.hotels.bdp.circustrain.replication.event"), is(EVENT_ID));
    assertThat(replica.getParameters().get("com.hotels.bdp.circustrain.last.replicated"), is(not(nullValue())));
    assertThat(replica.getParameters().get("com.hotels.bdp.circustrain.replication.mode"), is(FULL.name()));
    assertThat(replica.getParameters().get("DO_NOT_UPDATE_STATS"), is("true"));
    assertThat(replica.getParameters().get("STATS_GENERATED_VIA_STATS_TASK"), is("true"));
    assertThat(replica.getParameters().get("STATS_GENERATED"), is("true"));
    assertThat(replica.getParameters().get(StatsSetupConst.ROW_COUNT), is("1"));
  }

  @Test
  public void newPartitionWithTransformation() {
    ReplicaTableFactory factory = new ReplicaTableFactory(SOURCE_META_STORE_URIS, TableTransformation.IDENTITY,
        PARTITION_TRANSFORMATION, ColumnStatisticsTransformation.IDENTITY);

    Path replicaPartitionPath = new Path(REPLICA_DATA_DESTINATION, REPLICA_PARTITION_SUBPATH);
    Partition replica = factory.newReplicaPartition(EVENT_ID, sourceTable, sourcePartition, DB_NAME, TABLE_NAME,
        replicaPartitionPath, FULL);

    assertThat(replica.getDbName(), is(sourceTable.getDbName()));
    assertThat(replica.getTableName(), is(sourceTable.getTableName()));
    assertThat(replica.getSd().getInputFormat(), is("newInputFormat"));
    assertThat(replica.getSd().getOutputFormat(), is(OUTPUT_FORMAT));
    assertThat(replica.getSd().getLocation(), is(replicaPartitionPath.toUri().toString()));
    assertThat(replica.getParameters().get("com.hotels.bdp.circustrain.source.table"), is(DB_NAME + "." + TABLE_NAME));
    assertThat(replica.getParameters().get("com.hotels.bdp.circustrain.source.metastore.uris"),
        is(SOURCE_META_STORE_URIS));
    assertThat(replica.getParameters().get("com.hotels.bdp.circustrain.source.location"), is(PARTITION_LOCATION));
    assertThat(replica.getParameters().get("com.hotels.bdp.circustrain.replication.event"), is(EVENT_ID));
    assertThat(replica.getParameters().get("com.hotels.bdp.circustrain.last.replicated"), is(not(nullValue())));
    assertThat(replica.getParameters().get("DO_NOT_UPDATE_STATS"), is("true"));
    assertThat(replica.getParameters().get("STATS_GENERATED_VIA_STATS_TASK"), is("true"));
    assertThat(replica.getParameters().get("STATS_GENERATED"), is("true"));
    assertThat(replica.getParameters().get(StatsSetupConst.ROW_COUNT), is("1"));
  }

  @Test
  public void newPartitionEncodedLocation() {
    Path replicaPartitionPath = new Path(REPLICA_DATA_DESTINATION, REPLICA_PARTITION_SUBPATH + "%25");
    Partition replica = factory.newReplicaPartition(EVENT_ID, sourceTable, sourcePartition, DB_NAME, TABLE_NAME,
        replicaPartitionPath, FULL);

    assertThat(replica.getSd().getLocation(),
        is("hdfs://someserver/replicaDataDestination/replicaPartitionLocation%25"));
  }

  @Test
  public void newTableWithNameMappings() {
    TableAndStatistics replicaAndStats = factory.newReplicaTable(EVENT_ID, sourceTableAndStats, MAPPED_DB_NAME,
        MAPPED_TABLE_NAME, REPLICA_DATA_DESTINATION, FULL);
    Table replica = replicaAndStats.getTable();

    assertThat(replica.getDbName(), is(MAPPED_DB_NAME));
    assertThat(replica.getTableName(), is(MAPPED_TABLE_NAME));
    assertThat(replica.getSd().getInputFormat(), is(INPUT_FORMAT));
    assertThat(replica.getSd().getOutputFormat(), is(OUTPUT_FORMAT));
    assertThat(replica.getSd().getLocation(), is(REPLICA_DATA_DESTINATION.toUri().toString()));
    assertThat(replica.getParameters().get("com.hotels.bdp.circustrain.source.table"), is(DB_NAME + "." + TABLE_NAME));
    assertThat(replica.getParameters().get("com.hotels.bdp.circustrain.source.metastore.uris"),
        is(SOURCE_META_STORE_URIS));
    assertThat(replica.getParameters().get("com.hotels.bdp.circustrain.source.location"), is(TABLE_LOCATION));
    assertThat(replica.getParameters().get("com.hotels.bdp.circustrain.replication.event"), is(EVENT_ID));
    assertThat(replica.getParameters().get("com.hotels.bdp.circustrain.last.replicated"), is(not(nullValue())));
    assertThat(replica.getParameters().get("DO_NOT_UPDATE_STATS"), is("true"));
    assertThat(replica.getParameters().get("STATS_GENERATED_VIA_STATS_TASK"), is("true"));
    assertThat(replica.getParameters().get("STATS_GENERATED"), is("true"));
  }

  @Test
  public void newPartitionNameMappings() {
    Path replicaPartitionPath = new Path(REPLICA_DATA_DESTINATION, REPLICA_PARTITION_SUBPATH);

    Partition replica = factory.newReplicaPartition(EVENT_ID, sourceTable, sourcePartition, MAPPED_DB_NAME,
        MAPPED_TABLE_NAME, replicaPartitionPath, FULL);

    assertThat(replica.getDbName(), is(MAPPED_DB_NAME));
    assertThat(replica.getTableName(), is(MAPPED_TABLE_NAME));
    assertThat(replica.getSd().getInputFormat(), is(INPUT_FORMAT));
    assertThat(replica.getSd().getOutputFormat(), is(OUTPUT_FORMAT));
    assertThat(replica.getSd().getLocation(), is(replicaPartitionPath.toUri().toString()));
    assertThat(replica.getParameters().get("com.hotels.bdp.circustrain.source.table"), is(DB_NAME + "." + TABLE_NAME));
    assertThat(replica.getParameters().get("com.hotels.bdp.circustrain.source.metastore.uris"),
        is(SOURCE_META_STORE_URIS));
    assertThat(replica.getParameters().get("com.hotels.bdp.circustrain.source.location"), is(PARTITION_LOCATION));
    assertThat(replica.getParameters().get("com.hotels.bdp.circustrain.replication.event"), is(EVENT_ID));
    assertThat(replica.getParameters().get("com.hotels.bdp.circustrain.last.replicated"), is(not(nullValue())));
    assertThat(replica.getParameters().get("DO_NOT_UPDATE_STATS"), is("true"));
    assertThat(replica.getParameters().get("STATS_GENERATED_VIA_STATS_TASK"), is("true"));
    assertThat(replica.getParameters().get("STATS_GENERATED"), is("true"));
  }

  @Test
  public void newTableWithNameMappingsAndStats() {
    ColumnStatisticsObj columnStatisticsObj1 = new ColumnStatisticsObj();
    ColumnStatisticsObj columnStatisticsObj2 = new ColumnStatisticsObj();
    List<ColumnStatisticsObj> columnStatisticsObjs = Arrays.asList(columnStatisticsObj1, columnStatisticsObj2);

    TableAndStatistics source = new TableAndStatistics(sourceTable,
        new ColumnStatistics(new ColumnStatisticsDesc(true, DB_NAME, TABLE_NAME), columnStatisticsObjs));

    TableAndStatistics replicaAndStats = factory.newReplicaTable(EVENT_ID, source, MAPPED_DB_NAME, MAPPED_TABLE_NAME,
        REPLICA_DATA_DESTINATION, FULL);
    Table replica = replicaAndStats.getTable();
    ColumnStatistics replicaStatistics = replicaAndStats.getStatistics();

    assertThat(replica.getDbName(), is(MAPPED_DB_NAME));
    assertThat(replica.getTableName(), is(MAPPED_TABLE_NAME));
    assertThat(replica.getSd().getInputFormat(), is(INPUT_FORMAT));
    assertThat(replica.getSd().getOutputFormat(), is(OUTPUT_FORMAT));
    assertThat(replica.getSd().getLocation(), is(REPLICA_DATA_DESTINATION.toUri().toString()));
    assertThat(replica.getParameters().get("com.hotels.bdp.circustrain.source.table"), is(DB_NAME + "." + TABLE_NAME));
    assertThat(replica.getParameters().get("com.hotels.bdp.circustrain.source.metastore.uris"),
        is(SOURCE_META_STORE_URIS));
    assertThat(replica.getParameters().get("com.hotels.bdp.circustrain.source.location"), is(TABLE_LOCATION));
    assertThat(replica.getParameters().get("com.hotels.bdp.circustrain.replication.event"), is(EVENT_ID));
    assertThat(replica.getParameters().get("com.hotels.bdp.circustrain.last.replicated"), is(not(nullValue())));
    assertThat(replica.getParameters().get("DO_NOT_UPDATE_STATS"), is("true"));
    assertThat(replica.getParameters().get("STATS_GENERATED_VIA_STATS_TASK"), is("true"));
    assertThat(replica.getParameters().get("STATS_GENERATED"), is("true"));

    assertThat(replicaStatistics.getStatsDesc().getDbName(), is(MAPPED_DB_NAME));
    assertThat(replicaStatistics.getStatsDesc().getTableName(), is(MAPPED_TABLE_NAME));
    assertThat(replicaStatistics.getStatsObj().size(), is(2));
    assertThat(replicaStatistics.getStatsObj().get(0), is(columnStatisticsObj1));
    assertThat(replicaStatistics.getStatsObj().get(1), is(columnStatisticsObj2));
  }

  @Test
  public void newReplicaPartitionStatistics() throws MetaException {
    sourceTable.setPartitionKeys(
        Arrays.asList(new FieldSchema("one", "string", null), new FieldSchema("two", "string", null)));

    Partition replicaPartition = new Partition(sourcePartition);
    replicaPartition.setDbName(MAPPED_DB_NAME);
    replicaPartition.setTableName(MAPPED_TABLE_NAME);
    replicaPartition.setValues(Arrays.asList("A", "B"));

    ColumnStatisticsObj columnStatisticsObj1 = new ColumnStatisticsObj();
    ColumnStatisticsObj columnStatisticsObj2 = new ColumnStatisticsObj();
    List<ColumnStatisticsObj> columnStatisticsObjs = Arrays.asList(columnStatisticsObj1, columnStatisticsObj2);

    ColumnStatisticsDesc columnStatisticsDesc = new ColumnStatisticsDesc(false, DB_NAME, TABLE_NAME);
    columnStatisticsDesc
        .setPartName(Warehouse.makePartName(sourceTable.getPartitionKeys(), replicaPartition.getValues()));

    ColumnStatistics sourcePartitionStatistics = new ColumnStatistics(columnStatisticsDesc, columnStatisticsObjs);

    ColumnStatistics replicaPartitionStatistics = factory.newReplicaPartitionStatistics(sourceTable, replicaPartition,
        sourcePartitionStatistics);

    assertThat(replicaPartitionStatistics.getStatsDesc().getDbName(), is(MAPPED_DB_NAME));
    assertThat(replicaPartitionStatistics.getStatsDesc().getTableName(), is(MAPPED_TABLE_NAME));
    assertThat(replicaPartitionStatistics.getStatsDesc().getPartName(), is("one=A/two=B"));
    assertThat(replicaPartitionStatistics.getStatsObj().size(), is(2));
    assertThat(replicaPartitionStatistics.getStatsObj().get(0), is(columnStatisticsObj1));
    assertThat(replicaPartitionStatistics.getStatsObj().get(1), is(columnStatisticsObj2));
  }

  @Test
  public void newReplicaPartitionStatisticsWithTransformation() throws MetaException {
    sourceTable.setPartitionKeys(
        Arrays.asList(new FieldSchema("one", "string", null), new FieldSchema("two", "string", null)));

    Partition replicaPartition = new Partition(sourcePartition);
    replicaPartition.setDbName(MAPPED_DB_NAME);
    replicaPartition.setTableName(MAPPED_TABLE_NAME);
    replicaPartition.setValues(Arrays.asList("A", "B"));

    ColumnStatisticsObj columnStatisticsObj1 = new ColumnStatisticsObj();
    ColumnStatisticsObj columnStatisticsObj2 = new ColumnStatisticsObj();
    List<ColumnStatisticsObj> columnStatisticsObjs = Arrays.asList(columnStatisticsObj1, columnStatisticsObj2);

    ColumnStatisticsDesc columnStatisticsDesc = new ColumnStatisticsDesc(false, DB_NAME, TABLE_NAME);
    columnStatisticsDesc
        .setPartName(Warehouse.makePartName(sourceTable.getPartitionKeys(), replicaPartition.getValues()));

    ColumnStatistics sourcePartitionStatistics = new ColumnStatistics(columnStatisticsDesc, columnStatisticsObjs);

    ReplicaTableFactory factory = new ReplicaTableFactory(SOURCE_META_STORE_URIS, TableTransformation.IDENTITY,
        PartitionTransformation.IDENTITY, COLUMN_STATISTICS_TRANSFORMATION);

    ColumnStatistics replicaPartitionStatistics = factory.newReplicaPartitionStatistics(sourceTable, replicaPartition,
        sourcePartitionStatistics);

    assertThat(replicaPartitionStatistics.getStatsDesc().getDbName(), is("new_db"));
    assertThat(replicaPartitionStatistics.getStatsDesc().getTableName(), is("new_table"));
    assertThat(replicaPartitionStatistics.getStatsDesc().getPartName(), is("part=newPart"));
    assertThat(replicaPartitionStatistics.getStatsObj().size(), is(2));
    assertThat(replicaPartitionStatistics.getStatsObj().get(0), is(columnStatisticsObj1));
    assertThat(replicaPartitionStatistics.getStatsObj().get(1), is(columnStatisticsObj2));
  }

  @Test
  public void newView() {
    sourceTableAndStats.getTable().setTableType(TableType.VIRTUAL_VIEW.name());
    sourceTableAndStats.getTable().getSd().setInputFormat(null);
    sourceTableAndStats.getTable().getSd().setOutputFormat(null);
    sourceTableAndStats.getTable().getSd().setLocation(null);

    TableAndStatistics replicaAndStats = factory.newReplicaTable(EVENT_ID, sourceTableAndStats, DB_NAME, TABLE_NAME,
        null, FULL);
    Table replica = replicaAndStats.getTable();

    assertThat(replica.getDbName(), is(sourceTable.getDbName()));
    assertThat(replica.getTableName(), is(sourceTable.getTableName()));
    assertThat(replica.getSd().getInputFormat(), is(nullValue()));
    assertThat(replica.getSd().getOutputFormat(), is(nullValue()));
    assertThat(replica.getSd().getLocation(), is(nullValue()));
    assertThat(replica.getParameters().get("com.hotels.bdp.circustrain.source.table"), is(DB_NAME + "." + TABLE_NAME));
    assertThat(replica.getParameters().get("com.hotels.bdp.circustrain.source.metastore.uris"),
        is(SOURCE_META_STORE_URIS));
    assertThat(replica.getParameters().get("com.hotels.bdp.circustrain.source.location"), is(""));
    assertThat(replica.getParameters().get("com.hotels.bdp.circustrain.replication.event"), is(EVENT_ID));
    assertThat(replica.getParameters().get("com.hotels.bdp.circustrain.last.replicated"), is(not(nullValue())));
    assertThat(replica.getParameters().get("com.hotels.bdp.circustrain.replication.mode"), is(FULL.name()));
    assertThat(replica.getParameters().get("DO_NOT_UPDATE_STATS"), is("true"));
    assertThat(replica.getParameters().get("STATS_GENERATED_VIA_STATS_TASK"), is("true"));
    assertThat(replica.getParameters().get("STATS_GENERATED"), is("true"));
    assertThat(replica.getParameters().get(StatsSetupConst.ROW_COUNT), is("1"));
    assertThat(replica.getTableType(), is(TableType.VIRTUAL_VIEW.name()));
    assertTrue(MetaStoreUtils.isView(replica));

    assertThat(replicaAndStats.getStatistics(), is(nullValue()));
  }

  @Test
  public void newViewPartition() {
    sourceTable.setTableType(TableType.VIRTUAL_VIEW.name());
    sourceTable.getSd().setInputFormat(null);
    sourceTable.getSd().setOutputFormat(null);
    sourceTable.getSd().setLocation(null);

    sourcePartition.getSd().setInputFormat(null);
    sourcePartition.getSd().setOutputFormat(null);
    sourcePartition.getSd().setLocation(null);

    Partition replica = factory.newReplicaPartition(EVENT_ID, sourceTable, sourcePartition, DB_NAME, TABLE_NAME, null,
        FULL);

    assertThat(replica.getDbName(), is(sourceTable.getDbName()));
    assertThat(replica.getTableName(), is(sourceTable.getTableName()));
    assertThat(replica.getSd().getInputFormat(), is(nullValue()));
    assertThat(replica.getSd().getOutputFormat(), is(nullValue()));
    assertThat(replica.getSd().getLocation(), is(nullValue()));
    assertThat(replica.getParameters().get("com.hotels.bdp.circustrain.source.table"), is(DB_NAME + "." + TABLE_NAME));
    assertThat(replica.getParameters().get("com.hotels.bdp.circustrain.source.metastore.uris"),
        is(SOURCE_META_STORE_URIS));
    assertThat(replica.getParameters().get("com.hotels.bdp.circustrain.source.location"), is(""));
    assertThat(replica.getParameters().get("com.hotels.bdp.circustrain.replication.event"), is(EVENT_ID));
    assertThat(replica.getParameters().get("com.hotels.bdp.circustrain.last.replicated"), is(not(nullValue())));
    assertThat(replica.getParameters().get("com.hotels.bdp.circustrain.replication.mode"), is(FULL.name()));
  }

}
