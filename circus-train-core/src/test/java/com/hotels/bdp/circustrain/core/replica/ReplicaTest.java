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
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import static com.hotels.bdp.circustrain.api.CircusTrainTableParameter.REPLICATION_EVENT;
import static com.hotels.bdp.circustrain.api.CircusTrainTableParameter.REPLICATION_MODE;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData._Fields;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsDesc;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.LongColumnStatsData;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SetPartitionsStatsRequest;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.base.Supplier;
import com.google.common.collect.Lists;

import com.hotels.bdp.circustrain.api.CircusTrainException;
import com.hotels.bdp.circustrain.api.ReplicaLocationManager;
import com.hotels.bdp.circustrain.api.SourceLocationManager;
import com.hotels.bdp.circustrain.api.conf.ReplicaCatalog;
import com.hotels.bdp.circustrain.api.conf.ReplicationMode;
import com.hotels.bdp.circustrain.api.event.ReplicaCatalogListener;
import com.hotels.bdp.circustrain.api.listener.HousekeepingListener;
import com.hotels.bdp.circustrain.api.metadata.ColumnStatisticsTransformation;
import com.hotels.bdp.circustrain.api.metadata.PartitionTransformation;
import com.hotels.bdp.circustrain.api.metadata.TableTransformation;
import com.hotels.bdp.circustrain.core.PartitionsAndStatistics;
import com.hotels.bdp.circustrain.core.TableAndStatistics;
import com.hotels.hcommon.hive.metastore.client.api.CloseableMetaStoreClient;

@RunWith(MockitoJUnitRunner.class)
public class ReplicaTest {

  private static final String NAME = "name";
  private static final String TABLE_NAME = "tableName";
  private static final String DB_NAME = "dbName";
  private static final String EVENT_ID = "eventId";
  private static final String SOURCE_META_STORE_URIS = "sourceMetaStoreUris";
  private static final String REPLICA_META_STORE_URIS = "replicaMetaStoreUris";
  private static final String COLUMN_A = "a";
  private static final String COLUMN_B = "b";
  private static final String COLUMN_C = "c";
  private static final String COLUMN_D = "d";
  private static final FieldSchema FIELD_A = new FieldSchema(COLUMN_A, "string", null);
  private static final FieldSchema FIELD_B = new FieldSchema(COLUMN_B, "string", null);
  private static final FieldSchema FIELD_C = new FieldSchema(COLUMN_C, "string", null);
  private static final FieldSchema FIELD_D = new FieldSchema(COLUMN_D, "string", null);
  private static final List<FieldSchema> FIELDS = Arrays.asList(FIELD_A, FIELD_B);
  private static final List<FieldSchema> PARTITIONS = Arrays.asList(FIELD_C, FIELD_D);

  public @Rule TemporaryFolder temporaryFolder = new TemporaryFolder();

  private @Mock ReplicaCatalog replicaCatalog;
  private @Mock Supplier<CloseableMetaStoreClient> metaStoreClientSupplier;
  private @Mock CloseableMetaStoreClient mockMetaStoreClient;
  private @Mock SourceLocationManager mockSourceLocationManager;
  private @Mock ReplicaLocationManager mockReplicaLocationManager;
  private @Captor ArgumentCaptor<List<Partition>> alterPartitionCaptor;
  private @Captor ArgumentCaptor<List<Partition>> addPartitionCaptor;
  private @Captor ArgumentCaptor<SetPartitionsStatsRequest> setStatsRequestCaptor;
  private @Mock HousekeepingListener houseKeepingListener;
  private @Mock ReplicaCatalogListener replicaCatalogListener;

  private final ReplicaTableFactory tableFactory = new ReplicaTableFactory(SOURCE_META_STORE_URIS,
      TableTransformation.IDENTITY, PartitionTransformation.IDENTITY, ColumnStatisticsTransformation.IDENTITY);
  private Replica replica;
  private TableAndStatistics tableAndStatistics;
  private Table sourceTable;
  private Table existingReplicaTable;
  private List<ColumnStatisticsObj> columnStatisticsObjs;
  private ColumnStatistics columnStatistics;
  private String tableLocation;
  private Partition existingPartition;
  private HiveConf hiveConf;

  @Before
  public void prepare() throws Exception {
    when(metaStoreClientSupplier.get()).thenReturn(mockMetaStoreClient);
    when(replicaCatalog.getName()).thenReturn(NAME);

    hiveConf = new HiveConf();
    hiveConf.setVar(ConfVars.METASTOREURIS, REPLICA_META_STORE_URIS);
    replica = newReplica(ReplicationMode.FULL);
    tableLocation = temporaryFolder.newFolder("table_location").toURI().toString();

    sourceTable = newTable();
    existingPartition = newPartition("one", "two");

    ColumnStatisticsObj columnStatisticsObj1 = new ColumnStatisticsObj(COLUMN_A, "string",
        new ColumnStatisticsData(_Fields.LONG_STATS, new LongColumnStatsData(0, 1)));
    ColumnStatisticsObj columnStatisticsObj2 = new ColumnStatisticsObj(COLUMN_B, "string",
        new ColumnStatisticsData(_Fields.LONG_STATS, new LongColumnStatsData(1, 2)));
    columnStatisticsObjs = Arrays.asList(columnStatisticsObj1, columnStatisticsObj2);
    ColumnStatisticsDesc statsDesc = new ColumnStatisticsDesc(true, DB_NAME, TABLE_NAME);
    columnStatistics = new ColumnStatistics(statsDesc, columnStatisticsObjs);

    tableAndStatistics = new TableAndStatistics(sourceTable, columnStatistics);

    existingReplicaTable = new Table(sourceTable);

    when(mockReplicaLocationManager.getTableLocation()).thenReturn(new Path(tableLocation));
    when(mockReplicaLocationManager.getPartitionBaseLocation()).thenReturn(new Path(tableLocation));

    when(mockMetaStoreClient.getTable(DB_NAME, TABLE_NAME)).thenReturn(existingReplicaTable);
  }

  private void convertSourceTableToView() {
    sourceTable.setTableType(TableType.VIRTUAL_VIEW.name());
    sourceTable.getSd().setLocation(null);
  }

  private void convertExistingReplicaTableToView() {
    when(mockReplicaLocationManager.getTableLocation()).thenReturn(null);
    when(mockReplicaLocationManager.getPartitionBaseLocation()).thenReturn(null);
    existingReplicaTable.setTableType(TableType.VIRTUAL_VIEW.name());
    existingReplicaTable.getSd().setLocation(null);
    existingPartition.getSd().setLocation(null);
  }

  private Replica newReplica(ReplicationMode replicationMode) {
    return new Replica(replicaCatalog, hiveConf, metaStoreClientSupplier, tableFactory, houseKeepingListener,
        replicaCatalogListener, replicationMode);
  }

  @Test
  public void alteringExistingUnpartitionedReplicaTableSucceeds() throws TException, IOException {
    existingReplicaTable.getParameters().put(REPLICATION_EVENT.parameterName(), "previousEventId");
    replica.updateMetadata(EVENT_ID, tableAndStatistics, DB_NAME, TABLE_NAME, mockReplicaLocationManager);
    verify(mockMetaStoreClient).alter_table(eq(DB_NAME), eq(TABLE_NAME), any(Table.class));
    verify(mockMetaStoreClient).updateTableColumnStatistics(columnStatistics);
  }

  @Test
  public void alteringExistingUnpartitionedReplicaTableWithNoStatsSucceeds() throws TException, IOException {
    tableAndStatistics = new TableAndStatistics(sourceTable, null);
    existingReplicaTable.getParameters().put(REPLICATION_EVENT.parameterName(), "previousEventId");
    replica.updateMetadata(EVENT_ID, tableAndStatistics, DB_NAME, TABLE_NAME, mockReplicaLocationManager);
    verify(mockMetaStoreClient).alter_table(eq(DB_NAME), eq(TABLE_NAME), any(Table.class));
    verify(mockMetaStoreClient, never()).updateTableColumnStatistics(any(ColumnStatistics.class));
  }

  @Test
  public void alteringExistingUnpartitionedReplicaViewSucceeds() throws TException, IOException {
    convertSourceTableToView();
    convertExistingReplicaTableToView();
    existingReplicaTable.getParameters().put(REPLICATION_EVENT.parameterName(), "previousEventId");
    replica.updateMetadata(EVENT_ID, tableAndStatistics, DB_NAME, TABLE_NAME, mockReplicaLocationManager);
    verify(mockMetaStoreClient).alter_table(eq(DB_NAME), eq(TABLE_NAME), any(Table.class));
    verify(mockReplicaLocationManager, never()).addCleanUpLocation(anyString(), any(Path.class));
  }

  @Test(expected = CircusTrainException.class)
  public void tryToReplaceExistingUnpartitionedReplicaTableWithView() throws TException, IOException {
    convertSourceTableToView();
    existingReplicaTable.getParameters().put(REPLICATION_EVENT.parameterName(), "previousEventId");
    replica.updateMetadata(EVENT_ID, tableAndStatistics, DB_NAME, TABLE_NAME, mockReplicaLocationManager);
  }

  @Test(expected = CircusTrainException.class)
  public void tryToReplaceExistingUnpartitionedReplicaViewWithTable() throws TException, IOException {
    convertExistingReplicaTableToView();
    existingReplicaTable.getParameters().put(REPLICATION_EVENT.parameterName(), "previousEventId");
    replica.updateMetadata(EVENT_ID, tableAndStatistics, DB_NAME, TABLE_NAME, mockReplicaLocationManager);
  }

  @Test(expected = DestinationNotReplicaException.class)
  public void validateReplicaTableOnNonReplicicatedTableFails() throws TException, IOException {
    try {
      replica.validateReplicaTable(DB_NAME, TABLE_NAME);
    } catch (DestinationNotReplicaException e) {
      // Check that nothing was written to the metastore
      verify(mockMetaStoreClient).getTable(DB_NAME, TABLE_NAME);
      verify(mockMetaStoreClient).close();
      verifyNoMoreInteractions(mockMetaStoreClient);
      throw e;
    }
  }

  @Test
  public void validateReplicaTableOnAMirroredTableFails() throws TException, IOException {
    try {
      existingReplicaTable.putToParameters(REPLICATION_EVENT.parameterName(), "previousEventId");
      existingReplicaTable.putToParameters(REPLICATION_MODE.parameterName(), ReplicationMode.METADATA_MIRROR.name());
      replica.validateReplicaTable(DB_NAME, TABLE_NAME);
      fail("Should have thrown InvalidReplicationModeException");
    } catch (InvalidReplicationModeException e) {
      // Check that nothing was written to the metastore
      verify(mockMetaStoreClient).getTable(DB_NAME, TABLE_NAME);
      verify(mockMetaStoreClient).close();
      verifyNoMoreInteractions(mockMetaStoreClient);
    }
  }

  @Test
  public void validateMetadataUpdateReplicaTableOnAMirroredTableFails() throws TException, IOException {
    try {
      existingReplicaTable.putToParameters(REPLICATION_EVENT.parameterName(), "previousEventId");
      existingReplicaTable.putToParameters(REPLICATION_MODE.parameterName(), ReplicationMode.METADATA_MIRROR.name());
      replica = newReplica(ReplicationMode.METADATA_UPDATE);
      replica.validateReplicaTable(DB_NAME, TABLE_NAME);
      fail("Should have thrown InvalidReplicationModeException");
    } catch (InvalidReplicationModeException e) {
      // Check that nothing was written to the metastore
      verify(mockMetaStoreClient).getTable(DB_NAME, TABLE_NAME);
      verify(mockMetaStoreClient).close();
      verifyNoMoreInteractions(mockMetaStoreClient);
    }
  }

  @Test
  public void validateReplicaTableMetadataMirrorOnExistingNoReplicationModeSetTableFails()
    throws TException, IOException {
    try {
      existingReplicaTable.putToParameters(REPLICATION_EVENT.parameterName(), "previousEventId");
      // no replicationMode set in existing table
      replica = newReplica(ReplicationMode.METADATA_MIRROR);
      replica.validateReplicaTable(DB_NAME, TABLE_NAME);
      fail("Should have thrown InvalidReplicationModeException");
    } catch (InvalidReplicationModeException e) {
      // Check that nothing was written to the metastore
      verify(mockMetaStoreClient).getTable(DB_NAME, TABLE_NAME);
      verify(mockMetaStoreClient).close();
      verifyNoMoreInteractions(mockMetaStoreClient);
    }
  }

  @Test
  public void validateReplicaTableMetadataMirrorOnExistingFullReplicationTableFails() throws TException, IOException {
    try {
      existingReplicaTable.putToParameters(REPLICATION_EVENT.parameterName(), "previousEventId");
      existingReplicaTable.putToParameters(REPLICATION_MODE.parameterName(), ReplicationMode.FULL.name());
      replica = newReplica(ReplicationMode.METADATA_MIRROR);
      replica.validateReplicaTable(DB_NAME, TABLE_NAME);
      fail("Should have thrown InvalidReplicationModeException");
    } catch (InvalidReplicationModeException e) {
      // Check that nothing was written to the metastore
      verify(mockMetaStoreClient).getTable(DB_NAME, TABLE_NAME);
      verify(mockMetaStoreClient).close();
      verifyNoMoreInteractions(mockMetaStoreClient);
    }
  }

  @Test
  public void validateReplicaTableMetadataMirrorOnExistingMetadataUpdateTableFails() throws TException, IOException {
    try {
      existingReplicaTable.putToParameters(REPLICATION_EVENT.parameterName(), "previousEventId");
      existingReplicaTable.putToParameters(REPLICATION_MODE.parameterName(), ReplicationMode.METADATA_UPDATE.name());
      replica = newReplica(ReplicationMode.METADATA_MIRROR);
      replica.validateReplicaTable(DB_NAME, TABLE_NAME);
      fail("Should have thrown InvalidReplicationModeException");
    } catch (InvalidReplicationModeException e) {
      // Check that nothing was written to the metastore
      verify(mockMetaStoreClient).getTable(DB_NAME, TABLE_NAME);
      verify(mockMetaStoreClient).close();
      verifyNoMoreInteractions(mockMetaStoreClient);
    }
  }

  @Test
  public void alteringExistingPartitionedReplicaTableSucceeds() throws TException, IOException {
    when(mockMetaStoreClient
        .getPartitionsByNames(DB_NAME, TABLE_NAME, Lists.newArrayList("c=one/d=two", "c=three/d=four")))
            .thenReturn(Arrays.asList(existingPartition));
    existingReplicaTable.getParameters().put(REPLICATION_EVENT.parameterName(), "previousEventId");
    replica
        .updateMetadata(EVENT_ID, tableAndStatistics,
            new PartitionsAndStatistics(sourceTable.getPartitionKeys(), Collections.<Partition>emptyList(),
                Collections.<String, List<ColumnStatisticsObj>>emptyMap()),
            DB_NAME, TABLE_NAME, mockReplicaLocationManager);
    verify(mockMetaStoreClient).alter_table(eq(DB_NAME), eq(TABLE_NAME), any(Table.class));
    verify(mockMetaStoreClient).updateTableColumnStatistics(columnStatistics);
  }

  @Test
  public void alteringExistingPartitionedReplicaViewSucceeds() throws TException, IOException {
    convertSourceTableToView();
    convertExistingReplicaTableToView();
    when(mockMetaStoreClient
        .getPartitionsByNames(DB_NAME, TABLE_NAME, Lists.newArrayList("c=one/d=two", "c=three/d=four")))
            .thenReturn(Arrays.asList(existingPartition));
    existingReplicaTable.getParameters().put(REPLICATION_EVENT.parameterName(), "previousEventId");
    replica
        .updateMetadata(EVENT_ID, tableAndStatistics,
            new PartitionsAndStatistics(sourceTable.getPartitionKeys(), Collections.<Partition>emptyList(),
                Collections.<String, List<ColumnStatisticsObj>>emptyMap()),
            DB_NAME, TABLE_NAME, mockReplicaLocationManager);
    verify(mockMetaStoreClient).alter_table(eq(DB_NAME), eq(TABLE_NAME), any(Table.class));
    verify(mockReplicaLocationManager, never()).addCleanUpLocation(anyString(), any(Path.class));
  }

  @Test(expected = CircusTrainException.class)
  public void tryToReplaceExistingPartitionedReplicaViewWithTable() throws TException, IOException {
    convertExistingReplicaTableToView();
    existingReplicaTable.getParameters().put(REPLICATION_EVENT.parameterName(), "previousEventId");
    replica
        .updateMetadata(EVENT_ID, tableAndStatistics,
            new PartitionsAndStatistics(sourceTable.getPartitionKeys(), Collections.<Partition>emptyList(),
                Collections.<String, List<ColumnStatisticsObj>>emptyMap()),
            DB_NAME, TABLE_NAME, mockReplicaLocationManager);
  }

  @Test(expected = CircusTrainException.class)
  public void tryToReplaceExistingPartitionedReplicaTableWithView() throws TException, IOException {
    convertSourceTableToView();
    existingReplicaTable.getParameters().put(REPLICATION_EVENT.parameterName(), "previousEventId");
    replica
        .updateMetadata(EVENT_ID, tableAndStatistics,
            new PartitionsAndStatistics(sourceTable.getPartitionKeys(), Collections.<Partition>emptyList(),
                Collections.<String, List<ColumnStatisticsObj>>emptyMap()),
            DB_NAME, TABLE_NAME, mockReplicaLocationManager);
  }

  @Test(expected = DestinationNotReplicaException.class)
  public void validateReplicaTableExistingNonReplicaPartitionedTableFails() throws TException, IOException {
    try {
      replica.validateReplicaTable(DB_NAME, TABLE_NAME);
    } catch (DestinationNotReplicaException e) {
      // Check that nothing was written to the metastore
      verify(mockMetaStoreClient).getTable(DB_NAME, TABLE_NAME);
      verify(mockMetaStoreClient).close();
      verifyNoMoreInteractions(mockMetaStoreClient);
      throw e;
    }
  }

  @Test
  public void getName() throws Exception {
    assertThat(replica.getName(), is(NAME));
  }

  @Test
  public void alteringExistingPartitionedReplicaTableWithPartitionsSucceeds() throws TException, IOException {
    Partition newPartition = newPartition("three", "four");
    ColumnStatistics newPartitionStatistics = newPartitionStatistics("three", "four");
    Partition modifiedPartition = new Partition(existingPartition);
    ColumnStatistics modifiedPartitionStatistics = newPartitionStatistics("one", "two");
    when(mockMetaStoreClient
        .getPartitionsByNames(DB_NAME, TABLE_NAME, Lists.newArrayList("c=one/d=two", "c=three/d=four")))
            .thenReturn(Arrays.asList(existingPartition));

    Map<String, List<ColumnStatisticsObj>> partitionStatsMap = new HashMap<>();
    partitionStatsMap
        .put(Warehouse.makePartName(PARTITIONS, newPartition.getValues()), newPartitionStatistics.getStatsObj());
    partitionStatsMap
        .put(Warehouse.makePartName(PARTITIONS, modifiedPartition.getValues()),
            modifiedPartitionStatistics.getStatsObj());

    PartitionsAndStatistics partitionsAndStatistics = new PartitionsAndStatistics(sourceTable.getPartitionKeys(),
        Arrays.asList(modifiedPartition, newPartition), partitionStatsMap);
    when(mockReplicaLocationManager.getPartitionLocation(existingPartition))
        .thenReturn(new Path(tableLocation, "c=one/d=two"));
    when(mockReplicaLocationManager.getPartitionLocation(newPartition))
        .thenReturn(new Path(tableLocation, "c=three/d=four"));

    existingReplicaTable.getParameters().put(REPLICATION_EVENT.parameterName(), "previousEventId");

    replica
        .updateMetadata(EVENT_ID, tableAndStatistics, partitionsAndStatistics, DB_NAME, TABLE_NAME,
            mockReplicaLocationManager);

    verify(mockMetaStoreClient).alter_table(eq(DB_NAME), eq(TABLE_NAME), any(Table.class));
    verify(mockMetaStoreClient).updateTableColumnStatistics(columnStatistics);
    verify(mockMetaStoreClient).alter_partitions(eq(DB_NAME), eq(TABLE_NAME), alterPartitionCaptor.capture());
    verify(mockMetaStoreClient).add_partitions(addPartitionCaptor.capture());

    assertThat(alterPartitionCaptor.getValue().size(), is(1));
    assertThat(addPartitionCaptor.getValue().size(), is(1));

    Partition altered = alterPartitionCaptor.getValue().get(0);
    assertThat(altered.getValues(), is(Arrays.asList("one", "two")));

    Partition added = addPartitionCaptor.getValue().get(0);
    assertThat(added.getValues(), is(Arrays.asList("three", "four")));

    verify(mockMetaStoreClient).setPartitionColumnStatistics(setStatsRequestCaptor.capture());
    SetPartitionsStatsRequest statsRequest = setStatsRequestCaptor.getValue();

    List<ColumnStatistics> columnStats = new ArrayList<>(statsRequest.getColStats());
    Collections.sort(columnStats, new Comparator<ColumnStatistics>() {
      @Override
      public int compare(ColumnStatistics o1, ColumnStatistics o2) {
        return o1.getStatsDesc().getPartName().compareTo(o2.getStatsDesc().getPartName());
      }
    });
    assertThat(columnStats.size(), is(2));

    assertThat(columnStats.get(0).getStatsDesc().isIsTblLevel(), is(false));
    assertThat(columnStats.get(0).getStatsDesc().getDbName(), is(DB_NAME));
    assertThat(columnStats.get(0).getStatsDesc().getTableName(), is(TABLE_NAME));
    assertThat(columnStats.get(0).getStatsDesc().getPartName(), is("c=one/d=two"));
    assertThat(columnStats.get(0).getStatsObj().size(), is(2));
    assertThat(columnStats.get(1).getStatsDesc().isIsTblLevel(), is(false));
    assertThat(columnStats.get(1).getStatsDesc().getDbName(), is(DB_NAME));
    assertThat(columnStats.get(1).getStatsDesc().getTableName(), is(TABLE_NAME));
    assertThat(columnStats.get(1).getStatsDesc().getPartName(), is("c=three/d=four"));
    assertThat(columnStats.get(1).getStatsObj().size(), is(2));
  }

  @Test
  public void alteringExistingPartitionedReplicaViewWithPartitionsSucceeds() throws TException, IOException {
    convertSourceTableToView();
    convertExistingReplicaTableToView();
    Partition newPartition = newPartition("three", "four");
    newPartition.getSd().setLocation(null);
    ColumnStatistics newPartitionStatistics = newPartitionStatistics("three", "four");
    Partition modifiedPartition = new Partition(existingPartition);
    ColumnStatistics modifiedPartitionStatistics = newPartitionStatistics("one", "two");
    when(mockMetaStoreClient
        .getPartitionsByNames(DB_NAME, TABLE_NAME, Lists.newArrayList("c=one/d=two", "c=three/d=four")))
            .thenReturn(Arrays.asList(existingPartition));

    Map<String, List<ColumnStatisticsObj>> partitionStatsMap = new HashMap<>();
    partitionStatsMap
        .put(Warehouse.makePartName(PARTITIONS, newPartition.getValues()), newPartitionStatistics.getStatsObj());
    partitionStatsMap
        .put(Warehouse.makePartName(PARTITIONS, modifiedPartition.getValues()),
            modifiedPartitionStatistics.getStatsObj());

    PartitionsAndStatistics partitionsAndStatistics = new PartitionsAndStatistics(sourceTable.getPartitionKeys(),
        Arrays.asList(modifiedPartition, newPartition), partitionStatsMap);

    existingReplicaTable.getParameters().put(REPLICATION_EVENT.parameterName(), "previousEventId");

    replica
        .updateMetadata(EVENT_ID, tableAndStatistics, partitionsAndStatistics, DB_NAME, TABLE_NAME,
            mockReplicaLocationManager);

    verify(mockMetaStoreClient).alter_table(eq(DB_NAME), eq(TABLE_NAME), any(Table.class));
    verify(mockMetaStoreClient).updateTableColumnStatistics(columnStatistics);
    verify(mockMetaStoreClient).alter_partitions(eq(DB_NAME), eq(TABLE_NAME), alterPartitionCaptor.capture());
    verify(mockMetaStoreClient).add_partitions(addPartitionCaptor.capture());
    verify(mockReplicaLocationManager, never()).addCleanUpLocation(anyString(), any(Path.class));

    assertThat(alterPartitionCaptor.getValue().size(), is(1));
    assertThat(addPartitionCaptor.getValue().size(), is(1));

    Partition altered = alterPartitionCaptor.getValue().get(0);
    assertThat(altered.getValues(), is(Arrays.asList("one", "two")));

    Partition added = addPartitionCaptor.getValue().get(0);
    assertThat(added.getValues(), is(Arrays.asList("three", "four")));

    verify(mockMetaStoreClient).setPartitionColumnStatistics(setStatsRequestCaptor.capture());
    SetPartitionsStatsRequest statsRequest = setStatsRequestCaptor.getValue();

    List<ColumnStatistics> columnStats = new ArrayList<>(statsRequest.getColStats());
    Collections.sort(columnStats, new Comparator<ColumnStatistics>() {
      @Override
      public int compare(ColumnStatistics o1, ColumnStatistics o2) {
        return o1.getStatsDesc().getPartName().compareTo(o2.getStatsDesc().getPartName());
      }
    });
    assertThat(columnStats.size(), is(2));

    assertThat(columnStats.get(0).getStatsDesc().isIsTblLevel(), is(false));
    assertThat(columnStats.get(0).getStatsDesc().getDbName(), is(DB_NAME));
    assertThat(columnStats.get(0).getStatsDesc().getTableName(), is(TABLE_NAME));
    assertThat(columnStats.get(0).getStatsDesc().getPartName(), is("c=one/d=two"));
    assertThat(columnStats.get(0).getStatsObj().size(), is(2));
    assertThat(columnStats.get(1).getStatsDesc().isIsTblLevel(), is(false));
    assertThat(columnStats.get(1).getStatsDesc().getDbName(), is(DB_NAME));
    assertThat(columnStats.get(1).getStatsDesc().getTableName(), is(TABLE_NAME));
    assertThat(columnStats.get(1).getStatsDesc().getPartName(), is("c=three/d=four"));
    assertThat(columnStats.get(1).getStatsObj().size(), is(2));
  }

  @Test
  public void updateMetadataCalledWithoutPartitionsDoesCleanUpLocations() throws TException, IOException {
    existingReplicaTable.getParameters().put(REPLICATION_EVENT.parameterName(), "previousEventId");
    replica.updateMetadata(EVENT_ID, tableAndStatistics, DB_NAME, TABLE_NAME, mockReplicaLocationManager);
    verify(mockMetaStoreClient).alter_table(eq(DB_NAME), eq(TABLE_NAME), any(Table.class));
    verify(mockMetaStoreClient).updateTableColumnStatistics(columnStatistics);
    verify(mockReplicaLocationManager, never()).addCleanUpLocation(anyString(), any(Path.class));
  }

  private Table newTable() {
    Table table = new Table();
    table.setDbName(DB_NAME);
    table.setTableName(TABLE_NAME);
    table.setTableType(TableType.EXTERNAL_TABLE.name());

    StorageDescriptor sd = new StorageDescriptor();
    sd.setLocation(tableLocation);
    table.setSd(sd);

    HashMap<String, String> parameters = new HashMap<>();
    parameters.put(StatsSetupConst.ROW_COUNT, "1");
    table.setParameters(parameters);

    table.setPartitionKeys(PARTITIONS);
    return table;
  }

  private Partition newPartition(String... values) {
    Partition partition = new Partition();
    partition.setDbName(DB_NAME);
    partition.setTableName(TABLE_NAME);
    StorageDescriptor sd = new StorageDescriptor();
    sd.setLocation(new Path(tableLocation, partitionName(values)).toUri().toString());
    sd.setCols(FIELDS);
    partition.setSd(sd);
    HashMap<String, String> parameters = new HashMap<>();
    parameters.put(StatsSetupConst.ROW_COUNT, "1");
    partition.setParameters(parameters);
    partition.setValues(Arrays.asList(values));
    return partition;
  }

  private String partitionName(String... values) {
    try {
      return Warehouse.makePartName(PARTITIONS, Arrays.asList(values));
    } catch (MetaException e) {
      throw new RuntimeException(e);
    }
  }

  private ColumnStatistics newPartitionStatistics(String... values) {
    ColumnStatisticsObj columnStatisticsObj1 = new ColumnStatisticsObj(COLUMN_A, "string",
        new ColumnStatisticsData(_Fields.LONG_STATS, new LongColumnStatsData(0, 1)));
    ColumnStatisticsObj columnStatisticsObj2 = new ColumnStatisticsObj(COLUMN_B, "string",
        new ColumnStatisticsData(_Fields.LONG_STATS, new LongColumnStatsData(1, 2)));
    List<ColumnStatisticsObj> columnStatisticsObjs = Arrays.asList(columnStatisticsObj1, columnStatisticsObj2);
    ColumnStatisticsDesc statsDesc = new ColumnStatisticsDesc(false, DB_NAME, TABLE_NAME);
    statsDesc.setPartName(partitionName(values));
    return new ColumnStatistics(statsDesc, columnStatisticsObjs);
  }

}
