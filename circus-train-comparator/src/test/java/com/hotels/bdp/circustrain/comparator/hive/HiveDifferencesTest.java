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
package com.hotels.bdp.circustrain.comparator.hive;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyList;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.base.Function;
import com.google.common.base.Optional;

import com.hotels.bdp.circustrain.api.CircusTrainTableParameter;
import com.hotels.bdp.circustrain.comparator.ComparatorRegistry;
import com.hotels.bdp.circustrain.comparator.TestUtils;
import com.hotels.bdp.circustrain.comparator.api.BaseDiff;
import com.hotels.bdp.circustrain.comparator.api.Comparator;
import com.hotels.bdp.circustrain.comparator.api.Diff;
import com.hotels.bdp.circustrain.comparator.api.DiffListener;
import com.hotels.bdp.circustrain.comparator.hive.wrappers.PartitionAndMetadata;
import com.hotels.bdp.circustrain.comparator.hive.wrappers.TableAndMetadata;
import com.hotels.bdp.circustrain.hive.fetcher.PartitionFetcher;
import com.hotels.bdp.circustrain.hive.fetcher.PartitionNotFoundException;

@RunWith(MockitoJUnitRunner.class)
public class HiveDifferencesTest {

  private static final String DATABASE = "db";
  private static final String SOURCE_TABLE = "source";
  private static final String SOURCE_TABLE_LOCATION = "source_location";
  private static final String SOURCE_PARTITION_LOCATION = SOURCE_TABLE_LOCATION + "/a=01/";
  private static final String REPLICA_TABLE = "replica";
  private static final String REPLICA_TABLE_LOCATION = "replica_location";
  private static final String REPLICA_PARTITION_LOCATION = REPLICA_TABLE_LOCATION + "/a=01/";

  private final Configuration sourceConfiguration = new Configuration();
  private @Mock DiffListener diffListener;
  private @Mock ComparatorRegistry comparatorRegistry;

  private Table sourceTable;
  private Partition source01;
  private @Mock Iterator<Partition> sourcePartitionIterable;

  private Table replicaTable;
  private Partition replica01;
  private @Mock PartitionFetcher replicaPartitionFetcher;

  private @Mock Comparator<TableAndMetadata, Object> tableAndMetadataComparator;
  private @Mock Comparator<PartitionAndMetadata, Object> partitionAndMetadataComparator;
  private @Mock Function<Path, String> checksumFunction;

  private HiveDifferences hiveDifferences;

  private static Table newTable(String databaseName, String tableName, String location) {
    Table table = new Table();

    table.setDbName(databaseName);
    table.setTableName(tableName);
    table.setParameters(new HashMap<String, String>());
    table.setPartitionKeys(Arrays.asList(new FieldSchema("a", "string", null)));

    StorageDescriptor sd = new StorageDescriptor();
    sd.setLocation(location);
    table.setSd(sd);

    return table;
  }

  private static Partition newPartition(String databaseName, String tableName, String location) {
    Partition partition = new Partition();

    partition.setDbName(databaseName);
    partition.setTableName(tableName);
    partition.setParameters(new HashMap<String, String>());
    partition.setValues(Arrays.asList("01"));

    StorageDescriptor sd = new StorageDescriptor();
    sd.setLocation(location);
    partition.setSd(sd);

    return partition;
  }

  @Before
  public void init() {
    doReturn(tableAndMetadataComparator).when(comparatorRegistry).comparatorFor(TableAndMetadata.class);
    doReturn(partitionAndMetadataComparator).when(comparatorRegistry).comparatorFor(PartitionAndMetadata.class);

    sourceTable = newTable(DATABASE, SOURCE_TABLE, SOURCE_TABLE_LOCATION);
    source01 = newPartition(DATABASE, SOURCE_TABLE, SOURCE_PARTITION_LOCATION);
    when(sourcePartitionIterable.hasNext()).thenReturn(true, false);
    when(sourcePartitionIterable.next()).thenReturn(source01);

    replicaTable = newTable(DATABASE, REPLICA_TABLE, REPLICA_TABLE_LOCATION);
    replicaTable.getParameters().put(CircusTrainTableParameter.SOURCE_TABLE.parameterName(),
        DATABASE + "." + SOURCE_TABLE);
    replicaTable.getParameters().put(CircusTrainTableParameter.SOURCE_LOCATION.parameterName(), SOURCE_TABLE);
    replica01 = newPartition(DATABASE, REPLICA_TABLE, REPLICA_PARTITION_LOCATION);
    replica01.getParameters().put(CircusTrainTableParameter.PARTITION_CHECKSUM.parameterName(), "checksum");
    when(replicaPartitionFetcher.fetch("a=01")).thenReturn(replica01);

    when(checksumFunction.apply(new Path(SOURCE_PARTITION_LOCATION))).thenReturn("checksum");

    when(tableAndMetadataComparator.compare(any(TableAndMetadata.class), any(TableAndMetadata.class)))
        .thenReturn(Collections.<Diff<Object, Object>> emptyList());
    when(partitionAndMetadataComparator.compare(any(PartitionAndMetadata.class), any(PartitionAndMetadata.class)))
        .thenReturn(Collections.<Diff<Object, Object>> emptyList());

    hiveDifferences = HiveDifferences
        .builder(diffListener)
        .comparatorRegistry(comparatorRegistry)
        .source(sourceConfiguration, sourceTable, sourcePartitionIterable)
        .replica(Optional.of(replicaTable), Optional.of(replicaPartitionFetcher))
        .checksumFunction(checksumFunction)
        .build();
  }

  @Test
  public void tablesMatch() {
    hiveDifferences.run();
    InOrder inOrder = inOrder(diffListener);
    inOrder.verify(diffListener).onDiffStart(any(TableAndMetadata.class), any(Optional.class));
    verify(diffListener, never()).onChangedTable(anyList());
    verify(diffListener, never()).onNewPartition(anyString(), any(Partition.class));
    verify(diffListener, never()).onChangedPartition(anyString(), any(Partition.class), anyList());
    verify(diffListener, never()).onDataChanged(anyString(), any(Partition.class));
    inOrder.verify(diffListener).onDiffEnd();
  }

  @Test
  public void tableDifferent() {
    reset(tableAndMetadataComparator);
    Diff<Object, Object> tableDiff = new BaseDiff<Object, Object>("tables are different", null, null);
    when(tableAndMetadataComparator.compare(any(TableAndMetadata.class), any(TableAndMetadata.class)))
        .thenReturn(Arrays.asList(tableDiff));

    hiveDifferences.run();
    InOrder inOrder = inOrder(diffListener);
    inOrder.verify(diffListener).onDiffStart(any(TableAndMetadata.class), any(Optional.class));
    inOrder.verify(diffListener, times(1)).onChangedTable(Arrays.asList(tableDiff));
    verify(diffListener, never()).onNewPartition(anyString(), any(Partition.class));
    verify(diffListener, never()).onChangedPartition(anyString(), any(Partition.class), anyList());
    verify(diffListener, never()).onDataChanged(anyString(), any(Partition.class));
    inOrder.verify(diffListener).onDiffEnd();
  }

  @Test
  public void newPartition() {
    reset(replicaPartitionFetcher);
    when(replicaPartitionFetcher.fetch("a=01")).thenThrow(new PartitionNotFoundException(""));

    hiveDifferences.run();
    InOrder inOrder = inOrder(diffListener);
    inOrder.verify(diffListener).onDiffStart(any(TableAndMetadata.class), any(Optional.class));
    verify(diffListener, never()).onChangedTable(anyList());
    inOrder.verify(diffListener, times(1)).onNewPartition("a=01", source01);
    verify(diffListener, never()).onChangedPartition(anyString(), any(Partition.class), anyList());
    verify(diffListener, never()).onDataChanged(anyString(), any(Partition.class));
    inOrder.verify(diffListener).onDiffEnd();
  }

  @Test
  public void partitionHasChanged() {
    reset(partitionAndMetadataComparator);
    Diff<Object, Object> tableDiff = new BaseDiff<Object, Object>("partitions are different", null, null);
    when(partitionAndMetadataComparator.compare(any(PartitionAndMetadata.class), any(PartitionAndMetadata.class)))
        .thenReturn(Arrays.asList(tableDiff));

    hiveDifferences.run();
    InOrder inOrder = inOrder(diffListener);
    inOrder.verify(diffListener).onDiffStart(any(TableAndMetadata.class), any(Optional.class));
    verify(diffListener, never()).onChangedTable(anyList());
    verify(diffListener, never()).onNewPartition(anyString(), any(Partition.class));
    inOrder.verify(diffListener, times(1)).onChangedPartition("a=01", source01, Arrays.asList(tableDiff));
    verify(diffListener, never()).onDataChanged(anyString(), any(Partition.class));
    inOrder.verify(diffListener).onDiffEnd();
  }

  @Test
  public void partitionDataHaveChanged() {
    reset(checksumFunction);
    when(checksumFunction.apply(new Path(SOURCE_PARTITION_LOCATION))).thenReturn("new checksum");

    hiveDifferences.run();
    InOrder inOrder = inOrder(diffListener);
    inOrder.verify(diffListener).onDiffStart(any(TableAndMetadata.class), any(Optional.class));
    verify(diffListener, never()).onChangedTable(anyList());
    verify(diffListener, never()).onNewPartition(anyString(), any(Partition.class));
    verify(diffListener, never()).onChangedPartition(anyString(), any(Partition.class), anyList());
    inOrder.verify(diffListener, times(1)).onDataChanged("a=01", source01);
    inOrder.verify(diffListener).onDiffEnd();
  }

  @Test
  public void sourceTableToTableAndMetadata() {
    Table sourceTable = TestUtils.newTable("sourceDB", "sourceTable");
    TableAndMetadata sourceTableAndMetadata = HiveDifferences.sourceTableToTableAndMetadata(sourceTable);
    assertThat(sourceTableAndMetadata.getSourceTable(), is("sourceDB.sourceTable"));
    assertThat(sourceTableAndMetadata.getSourceLocation(), is(sourceTable.getSd().getLocation()));
    assertThat(sourceTableAndMetadata.getTable(), is(sourceTable));
  }

  @Test
  public void sourcePartitionToPartitionAndMetadata() {
    Partition sourcePartition = TestUtils.newPartition("sourceDB", "sourceTable", "val");
    PartitionAndMetadata sourcePartitionAndMetadata = HiveDifferences
        .sourcePartitionToPartitionAndMetadata(sourcePartition);
    assertThat(sourcePartitionAndMetadata.getSourceTable(), is("sourceDB.sourceTable"));
    assertThat(sourcePartitionAndMetadata.getSourceLocation(), is(sourcePartition.getSd().getLocation()));
    assertThat(sourcePartitionAndMetadata.getPartition(), is(sourcePartition));
  }

  @Test
  public void replicaTableToTableAndMetadata() {
    Table sourceTable = TestUtils.newTable("sourceDB", "sourceTable");
    Table replicaTable = TestUtils.newTable("replicaDB", "replicaTable");
    TestUtils.setCircusTrainSourceParameters(sourceTable, replicaTable);
    TableAndMetadata replicaTableAndMetadata = HiveDifferences.replicaTableToTableAndMetadata(replicaTable);
    assertThat(replicaTableAndMetadata.getSourceTable(), is("sourceDB.sourceTable"));
    assertThat(replicaTableAndMetadata.getSourceLocation(), is(sourceTable.getSd().getLocation()));
    assertThat(replicaTableAndMetadata.getTable(), is(replicaTable));
    assertThat(replicaTableAndMetadata.getTable().getDbName(), is("replicaDB"));
    assertThat(replicaTableAndMetadata.getTable().getTableName(), is("replicaTable"));
  }

  @Test
  public void replicaPartitionToPartitionAndMetadata() {
    Partition sourcePartition = TestUtils.newPartition("sourceDB", "sourceTable", "val");
    Partition replicaPartition = TestUtils.newPartition("replicaDB", "replicaTable", "val");
    TestUtils.setCircusTrainSourceParameters(sourcePartition, replicaPartition);
    PartitionAndMetadata replicaPartitionAndMetadata = HiveDifferences
        .replicaPartitionToPartitionAndMetadata(replicaPartition);
    assertThat(replicaPartitionAndMetadata.getSourceTable(), is("sourceDB.sourceTable"));
    assertThat(replicaPartitionAndMetadata.getSourceLocation(), is(sourcePartition.getSd().getLocation()));
    assertThat(replicaPartitionAndMetadata.getPartition(), is(replicaPartition));
    assertThat(replicaPartitionAndMetadata.getPartition().getDbName(), is("replicaDB"));
    assertThat(replicaPartitionAndMetadata.getPartition().getTableName(), is("replicaTable"));
  }

  @Test(expected = IllegalStateException.class)
  public void requiredPartitionFetcher() {
    HiveDifferences
        .builder(diffListener)
        .comparatorRegistry(comparatorRegistry)
        .source(sourceConfiguration, sourceTable, sourcePartitionIterable)
        .replica(Optional.of(replicaTable), Optional.<PartitionFetcher> absent())
        .checksumFunction(checksumFunction)
        .build();
  }

  @Test
  public void replicaTableDoesNotExist() {
    hiveDifferences = HiveDifferences
        .builder(diffListener)
        .comparatorRegistry(comparatorRegistry)
        .source(sourceConfiguration, sourceTable, sourcePartitionIterable)
        .replica(Optional.<Table> absent(), Optional.<PartitionFetcher> absent())
        .checksumFunction(checksumFunction)
        .build();
    hiveDifferences.run();

    InOrder inOrder = inOrder(diffListener);
    inOrder.verify(diffListener).onDiffStart(any(TableAndMetadata.class), any(Optional.class));
    verify(diffListener, never()).onChangedTable(anyList());
    inOrder.verify(diffListener, times(1)).onNewPartition(anyString(), any(Partition.class));
    verify(diffListener, never()).onChangedPartition(anyString(), any(Partition.class), anyList());
    verify(diffListener, never()).onDataChanged(anyString(), any(Partition.class));
    inOrder.verify(diffListener).onDiffEnd();
  }
}
