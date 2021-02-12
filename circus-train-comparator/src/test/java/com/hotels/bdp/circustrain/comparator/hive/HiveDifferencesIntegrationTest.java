/**
 * Copyright (C) 2016-2021 Expedia, Inc.
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

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyList;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import static com.hotels.bdp.circustrain.comparator.api.ComparatorType.SHORT_CIRCUIT;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Optional;

import com.hotels.bdp.circustrain.api.CircusTrainTableParameter;
import com.hotels.bdp.circustrain.comparator.ComparatorRegistry;
import com.hotels.bdp.circustrain.comparator.api.BaseDiff;
import com.hotels.bdp.circustrain.comparator.api.Diff;
import com.hotels.bdp.circustrain.comparator.api.DiffListener;
import com.hotels.bdp.circustrain.hive.fetcher.BufferedPartitionFetcher;
import com.hotels.beeju.ThriftHiveMetaStoreJUnitRule;
import com.hotels.hcommon.hive.metastore.iterator.PartitionIterator;

@RunWith(MockitoJUnitRunner.class)
public class HiveDifferencesIntegrationTest {

  private static final Logger LOG = LoggerFactory.getLogger(HiveDifferencesIntegrationTest.class);

  private static final short PARTITION_BATCH_SIZE = (short) 3;
  private static final String PART_00000 = "part-00000";
  private static final String DATABASE = "ct_database";
  private static final String SOURCE_TABLE = "ct_table_1";
  private static final String REPLICA_TABLE = "ct_table_2";

  private static final FieldSchema BAR_COL = new FieldSchema("bar", "string", "");
  private static final FieldSchema FOO_COL = new FieldSchema("foo", "bigint", "");
  private static final FieldSchema BAZ_COL = new FieldSchema("baz", "int", "");
  private static final FieldSchema PARTITION_COL = new FieldSchema("part", "int", "");

  public @Rule TemporaryFolder temporaryFolder = new TemporaryFolder();
  public @Rule ThriftHiveMetaStoreJUnitRule catalog = new ThriftHiveMetaStoreJUnitRule(DATABASE);

  private @Mock DiffListener diffListener;
  private @Mock Function<Path, String> checksumFunction;

  private File sourceWarehouseUri;
  private File sourceTableUri;
  private File replicaWarehouseUri;
  private File replicaTableUri;
  private final ComparatorRegistry comparatorRegistry = new ComparatorRegistry(SHORT_CIRCUIT);
  private final Configuration configuration = new Configuration();

  @Before
  public void init() throws Exception {
    // We mock the checksum function because LocalFileSystem doesn't compute checksums
    when(checksumFunction.apply(any(Path.class))).thenAnswer(new Answer<String>() {
      @Override
      public String answer(InvocationOnMock invocation) throws Throwable {
        Path path = (Path) invocation.getArguments()[0];
        return path.getName();
      }
    });

    sourceWarehouseUri = temporaryFolder.newFolder("source-warehouse");
    sourceTableUri = new File(sourceWarehouseUri, DATABASE + "/" + SOURCE_TABLE);
    createTable(DATABASE, SOURCE_TABLE, sourceTableUri, null, null, false);

    replicaWarehouseUri = temporaryFolder.newFolder("replica-warehouse");
    replicaTableUri = new File(replicaWarehouseUri, DATABASE + "/" + REPLICA_TABLE);
    createTable(DATABASE, REPLICA_TABLE, replicaTableUri, DATABASE + "." + SOURCE_TABLE,
        sourceTableUri.toURI().toString(), true);

    HiveMetaStoreClient client = catalog.client();
    LOG.info(">>>> Source Table = {}", client.getTable(DATABASE, SOURCE_TABLE));
    LOG.info(">>>> Replica Table = {}", client.getTable(DATABASE, REPLICA_TABLE));
  }

  private File createPartitionData(String partitionVal, File partitionLocation, List<String> data) throws IOException {
    File partition = new File(partitionLocation, partitionVal);
    File partitionData = new File(partition, PART_00000);
    FileUtils.writeLines(partitionData, data);
    return partition;
  }

  private void createTable(
      String databaseName,
      String tableName,
      File tableLocation,
      String sourceTable,
      String sourceLocation,
      boolean addChecksum)
    throws Exception {
    File partition0 = createPartitionData("part=0", tableLocation, Arrays.asList("1\tadam", "2\tsusan"));
    File partition1 = createPartitionData("part=1", tableLocation, Arrays.asList("3\tchun", "4\tkim"));

    Table table = new Table();
    table.setDbName(databaseName);
    table.setTableName(tableName);
    table.setTableType(TableType.EXTERNAL_TABLE.name());
    table.setParameters(new HashMap<String, String>());
    if (sourceTable != null) {
      table.getParameters().put(CircusTrainTableParameter.SOURCE_TABLE.parameterName(), sourceTable);
    }
    if (sourceLocation != null) {
      table.getParameters().put(CircusTrainTableParameter.SOURCE_LOCATION.parameterName(), sourceLocation);
    }

    List<FieldSchema> partitionColumns = Arrays.asList(PARTITION_COL);
    table.setPartitionKeys(partitionColumns);

    List<FieldSchema> dataColumns = Arrays.asList(FOO_COL, BAR_COL);

    StorageDescriptor sd = new StorageDescriptor();
    sd.setCols(dataColumns);
    sd.setLocation(tableLocation.toURI().toString());
    sd.setParameters(new HashMap<String, String>());
    sd.setSerdeInfo(new SerDeInfo());

    table.setSd(sd);

    HiveMetaStoreClient client = catalog.client();
    client.createTable(table);
    LOG
        .info(">>>> Partitions added: {}",
            +client
                .add_partitions(Arrays
                    .asList(
                        newPartition(databaseName, tableName, sd, Arrays.asList("0"), partition0, sourceTable,
                            sourceLocation + "part=0", addChecksum),
                        newPartition(databaseName, tableName, sd, Arrays.asList("1"), partition1, sourceTable,
                            sourceLocation + "part=1", addChecksum))));
  }

  private Partition newPartition(
      String datbase,
      String table,
      StorageDescriptor tableStorageDescriptor,
      List<String> values,
      File location,
      String sourceTable,
      String sourceLocation,
      boolean addChecksum) {
    Partition partition = new Partition();
    partition.setDbName(datbase);
    partition.setTableName(table);
    partition.setValues(values);
    partition.setSd(new StorageDescriptor(tableStorageDescriptor));
    partition.getSd().setLocation(location.toURI().toString());
    partition.setParameters(new HashMap<String, String>());
    if (sourceTable != null) {
      partition.getParameters().put(CircusTrainTableParameter.SOURCE_TABLE.parameterName(), sourceTable);
    }
    if (sourceLocation != null) {
      partition.getParameters().put(CircusTrainTableParameter.SOURCE_LOCATION.parameterName(), sourceLocation);
    }
    if (addChecksum) {
      partition.getParameters().put(CircusTrainTableParameter.PARTITION_CHECKSUM.parameterName(), location.getName());
    }
    return partition;
  }

  @Test
  public void tablesMatchEachOther() throws Exception {
    Table sourceTable = catalog.client().getTable(DATABASE, SOURCE_TABLE);
    Table replicaTable = catalog.client().getTable(DATABASE, REPLICA_TABLE);

    HiveDifferences
        .builder(diffListener)
        .comparatorRegistry(comparatorRegistry)
        .source(configuration, sourceTable, new PartitionIterator(catalog.client(), sourceTable, PARTITION_BATCH_SIZE))
        .replica(Optional.of(replicaTable),
            Optional.of(new BufferedPartitionFetcher(catalog.client(), replicaTable, PARTITION_BATCH_SIZE)))
        .checksumFunction(checksumFunction)
        .build()
        .run();
    verify(diffListener, never()).onChangedTable(anyList());
    verify(diffListener, never()).onNewPartition(anyString(), any(Partition.class));
    verify(diffListener, never()).onChangedPartition(anyString(), any(Partition.class), anyList());
    verify(diffListener, never()).onDataChanged(anyString(), any(Partition.class));
  }

  @Test
  public void tablesAreDifferent() throws Exception {
    Table sourceTable = catalog.client().getTable(DATABASE, SOURCE_TABLE);
    sourceTable.getParameters().put("com.company.team", "value");
    catalog.client().alter_table(DATABASE, SOURCE_TABLE, sourceTable);

    // Reload table object
    sourceTable = catalog.client().getTable(DATABASE, SOURCE_TABLE);
    Table replicaTable = catalog.client().getTable(DATABASE, REPLICA_TABLE);

    HiveDifferences
        .builder(diffListener)
        .comparatorRegistry(comparatorRegistry)
        .source(configuration, sourceTable, new PartitionIterator(catalog.client(), sourceTable, PARTITION_BATCH_SIZE))
        .replica(Optional.of(replicaTable),
            Optional.of(new BufferedPartitionFetcher(catalog.client(), replicaTable, PARTITION_BATCH_SIZE)))
        .checksumFunction(checksumFunction)
        .build()
        .run();
    verify(diffListener, times(1)).onChangedTable(anyList());
    verify(diffListener, never()).onNewPartition(anyString(), any(Partition.class));
    verify(diffListener, never()).onChangedPartition(anyString(), any(Partition.class), anyList());
    verify(diffListener, never()).onDataChanged(anyString(), any(Partition.class));
  }

  @Test
  public void newSourcePartition() throws Exception {
    Table sourceTable = catalog.client().getTable(DATABASE, SOURCE_TABLE);
    File sourcePartition2Location = createPartitionData("part=2", sourceTableUri,
        Arrays.asList("5\troberto", "6\tpedro"));
    Partition sourcePartition2 = newPartition(DATABASE, SOURCE_TABLE, sourceTable.getSd(), Arrays.asList("2"),
        sourcePartition2Location, null, null, false);
    catalog.client().add_partition(sourcePartition2);

    Table replicaTable = catalog.client().getTable(DATABASE, REPLICA_TABLE);

    HiveDifferences
        .builder(diffListener)
        .comparatorRegistry(comparatorRegistry)
        .source(configuration, sourceTable, new PartitionIterator(catalog.client(), sourceTable, PARTITION_BATCH_SIZE))
        .replica(Optional.of(replicaTable),
            Optional.of(new BufferedPartitionFetcher(catalog.client(), replicaTable, PARTITION_BATCH_SIZE)))
        .checksumFunction(checksumFunction)
        .build()
        .run();
    verify(diffListener, never()).onChangedTable(anyList());
    verify(diffListener, times(1))
        .onNewPartition("part=2", catalog.client().getPartition(DATABASE, SOURCE_TABLE, "part=2"));
    verify(diffListener, never()).onChangedPartition(anyString(), any(Partition.class), anyList());
    verify(diffListener, never()).onDataChanged(anyString(), any(Partition.class));
  }

  @Test
  public void multiple() throws Exception {
    Table sourceTable = catalog.client().getTable(DATABASE, SOURCE_TABLE);

    // changed partition
    Partition sourcePartition1 = catalog.client().getPartition(DATABASE, SOURCE_TABLE, "part=1");
    sourcePartition1.getSd().getCols().add(BAZ_COL);
    catalog.client().alter_partition(DATABASE, SOURCE_TABLE, sourcePartition1);

    // new
    File sourcePartition2Location = createPartitionData("part=2", sourceTableUri,
        Arrays.asList("5\troberto", "6\tpedro"));
    Partition sourcePartition2 = newPartition(DATABASE, SOURCE_TABLE, sourceTable.getSd(), Arrays.asList("2"),
        sourcePartition2Location, null, null, false);
    catalog.client().add_partition(sourcePartition2);

    // changed data
    reset(checksumFunction);
    when(checksumFunction.apply(any(Path.class))).thenAnswer(new Answer<String>() {
      @Override
      public String answer(InvocationOnMock invocation) throws Throwable {
        Path path = (Path) invocation.getArguments()[0];
        if ("part=0".equals(path.getName())) {
          return "new part=0 checksum";
        }
        return path.getName();
      }
    });

    Table replicaTable = catalog.client().getTable(DATABASE, REPLICA_TABLE);

    HiveDifferences
        .builder(diffListener)
        .comparatorRegistry(comparatorRegistry)
        .source(configuration, sourceTable, new PartitionIterator(catalog.client(), sourceTable, PARTITION_BATCH_SIZE))
        .replica(Optional.of(replicaTable),
            Optional.of(new BufferedPartitionFetcher(catalog.client(), replicaTable, PARTITION_BATCH_SIZE)))
        .checksumFunction(checksumFunction)
        .build()
        .run();
    verify(diffListener, never()).onChangedTable(anyList());
    verify(diffListener, times(1))
        .onNewPartition("part=2", catalog.client().getPartition(DATABASE, SOURCE_TABLE, "part=2"));
    verify(diffListener, times(1))
        .onChangedPartition("part=1", catalog.client().getPartition(DATABASE, SOURCE_TABLE, "part=1"), Arrays
            .<Diff<Object, Object>>asList(new BaseDiff<Object, Object>(
                "Collection partition.sd.cols of class java.util.ArrayList has different size: left.size()=3 and right.size()=2",
                Arrays.asList(FOO_COL, BAR_COL, BAZ_COL), Arrays.asList(FOO_COL, BAR_COL))));
    verify(diffListener, times(1))
        .onDataChanged("part=0", catalog.client().getPartition(DATABASE, SOURCE_TABLE, "part=0"));
  }

  @Test
  public void multipleApplyLimit() throws Exception {
    Table sourceTable = catalog.client().getTable(DATABASE, SOURCE_TABLE);

    // changed partition
    Partition sourcePartition1 = catalog.client().getPartition(DATABASE, SOURCE_TABLE, "part=1");
    sourcePartition1.getSd().getCols().add(BAZ_COL);
    catalog.client().alter_partition(DATABASE, SOURCE_TABLE, sourcePartition1);

    // new
    File sourcePartition2Location = createPartitionData("part=2", sourceTableUri,
        Arrays.asList("5\troberto", "6\tpedro"));
    Partition sourcePartition2 = newPartition(DATABASE, SOURCE_TABLE, sourceTable.getSd(), Arrays.asList("2"),
        sourcePartition2Location, null, null, false);
    catalog.client().add_partition(sourcePartition2);

    // changed data
    reset(checksumFunction);
    when(checksumFunction.apply(any(Path.class))).thenAnswer(new Answer<String>() {
      @Override
      public String answer(InvocationOnMock invocation) throws Throwable {
        Path path = (Path) invocation.getArguments()[0];
        if ("part=0".equals(path.getName())) {
          return "new part=0 checksum";
        }
        return path.getName();
      }
    });

    Table replicaTable = catalog.client().getTable(DATABASE, REPLICA_TABLE);

    HiveDifferences
        .builder(diffListener)
        .comparatorRegistry(comparatorRegistry)
        .source(configuration, sourceTable, new PartitionIterator(catalog.client(), sourceTable, PARTITION_BATCH_SIZE))
        .replica(Optional.of(replicaTable),
            Optional.of(new BufferedPartitionFetcher(catalog.client(), replicaTable, PARTITION_BATCH_SIZE)))
        .checksumFunction(checksumFunction)
        // set limit
        .partitionLimit(1)
        .build()
        .run();
    verify(diffListener, never()).onChangedTable(anyList());
    verify(diffListener, never()).onNewPartition(anyString(), any(Partition.class));
    verify(diffListener, never()).onChangedPartition(anyString(), any(Partition.class), anyList());
    verify(diffListener, times(1))
        .onDataChanged("part=0", catalog.client().getPartition(DATABASE, SOURCE_TABLE, "part=0"));
  }

  @Test
  public void newReplicaPartition() throws Exception {
    Table replicaTable = catalog.client().getTable(DATABASE, REPLICA_TABLE);
    File replicaPartition2Location = createPartitionData("part=2", replicaTableUri,
        Arrays.asList("5\troberto", "6\tpedro"));
    Partition replicaPartition2 = newPartition(DATABASE, REPLICA_TABLE, replicaTable.getSd(), Arrays.asList("2"),
        replicaPartition2Location, null, null, false);
    catalog.client().add_partition(replicaPartition2);

    Table sourceTable = catalog.client().getTable(DATABASE, SOURCE_TABLE);

    HiveDifferences
        .builder(diffListener)
        .comparatorRegistry(comparatorRegistry)
        .source(configuration, sourceTable, new PartitionIterator(catalog.client(), sourceTable, PARTITION_BATCH_SIZE))
        .replica(Optional.of(replicaTable),
            Optional.of(new BufferedPartitionFetcher(catalog.client(), replicaTable, PARTITION_BATCH_SIZE)))
        .checksumFunction(checksumFunction)
        .build()
        .run();
    verify(diffListener, never()).onChangedTable(anyList());
    verify(diffListener, never()).onNewPartition(anyString(), any(Partition.class));
    verify(diffListener, never()).onChangedPartition(anyString(), any(Partition.class), anyList());
    verify(diffListener, never()).onDataChanged(anyString(), any(Partition.class));
  }

  @Test
  public void sourcePartitionHasChanged() throws Exception {
    Partition sourcePartition1 = catalog.client().getPartition(DATABASE, SOURCE_TABLE, "part=1");
    sourcePartition1.getSd().getCols().add(BAZ_COL);
    catalog.client().alter_partition(DATABASE, SOURCE_TABLE, sourcePartition1);

    Table sourceTable = catalog.client().getTable(DATABASE, SOURCE_TABLE);
    Table replicaTable = catalog.client().getTable(DATABASE, REPLICA_TABLE);
    HiveDifferences
        .builder(diffListener)
        .comparatorRegistry(comparatorRegistry)
        .source(configuration, sourceTable, new PartitionIterator(catalog.client(), sourceTable, PARTITION_BATCH_SIZE))
        .replica(Optional.of(replicaTable),
            Optional.of(new BufferedPartitionFetcher(catalog.client(), replicaTable, PARTITION_BATCH_SIZE)))
        .checksumFunction(checksumFunction)
        .build()
        .run();

    verify(diffListener, never()).onChangedTable(anyList());
    verify(diffListener, never()).onNewPartition(anyString(), any(Partition.class));
    verify(diffListener, times(1))
        .onChangedPartition("part=1", catalog.client().getPartition(DATABASE, SOURCE_TABLE, "part=1"), Arrays
            .<Diff<Object, Object>>asList(new BaseDiff<Object, Object>(
                "Collection partition.sd.cols of class java.util.ArrayList has different size: left.size()=3 and right.size()=2",
                Arrays.asList(FOO_COL, BAR_COL, BAZ_COL), Arrays.asList(FOO_COL, BAR_COL))));
    verify(diffListener, never()).onDataChanged(anyString(), any(Partition.class));
  }

  @Test
  public void replicaPartitionHasChanged() throws Exception {
    Partition replicaPartition1 = catalog.client().getPartition(DATABASE, REPLICA_TABLE, "part=1");
    replicaPartition1.getSd().getCols().add(BAZ_COL);
    catalog.client().alter_partition(DATABASE, REPLICA_TABLE, replicaPartition1);

    Table sourceTable = catalog.client().getTable(DATABASE, SOURCE_TABLE);
    Table replicaTable = catalog.client().getTable(DATABASE, REPLICA_TABLE);

    HiveDifferences
        .builder(diffListener)
        .comparatorRegistry(comparatorRegistry)
        .source(configuration, sourceTable, new PartitionIterator(catalog.client(), sourceTable, PARTITION_BATCH_SIZE))
        .replica(Optional.of(replicaTable),
            Optional.of(new BufferedPartitionFetcher(catalog.client(), replicaTable, PARTITION_BATCH_SIZE)))
        .checksumFunction(checksumFunction)
        .build()
        .run();
    verify(diffListener, never()).onChangedTable(anyList());
    verify(diffListener, never()).onNewPartition(anyString(), any(Partition.class));
    verify(diffListener, times(1))
        .onChangedPartition("part=1", catalog.client().getPartition(DATABASE, SOURCE_TABLE, "part=1"), Arrays
            .<Diff<Object, Object>>asList(new BaseDiff<Object, Object>(
                "Collection partition.sd.cols of class java.util.ArrayList has different size: left.size()=2 and right.size()=3",
                Arrays.asList(FOO_COL, BAR_COL), Arrays.asList(FOO_COL, BAR_COL, BAZ_COL))));
    verify(diffListener, never()).onDataChanged(anyString(), any(Partition.class));
  }

  @Test
  public void replicaPartitionHasChangedButIgnorableParamter() throws Exception {
    Partition replicaPartition1 = catalog.client().getPartition(DATABASE, REPLICA_TABLE, "part=1");
    replicaPartition1.putToParameters("DO_NOT_UPDATE_STATS", "true");
    replicaPartition1.putToParameters("STATS_GENERATED_VIA_STATS_TASK", "true");
    replicaPartition1.putToParameters("STATS_GENERATED", "true");
    catalog.client().alter_partition(DATABASE, REPLICA_TABLE, replicaPartition1);
    Table sourceTable = catalog.client().getTable(DATABASE, SOURCE_TABLE);
    Table replicaTable = catalog.client().getTable(DATABASE, REPLICA_TABLE);
    replicaPartition1.putToParameters("DO_NOT_UPDATE_STATS", "true");
    replicaPartition1.putToParameters("STATS_GENERATED_VIA_STATS_TASK", "true");
    replicaPartition1.putToParameters("STATS_GENERATED", "true");
    catalog.client().alter_table(DATABASE, REPLICA_TABLE, replicaTable);

    HiveDifferences
        .builder(diffListener)
        .comparatorRegistry(comparatorRegistry)
        .source(configuration, sourceTable, new PartitionIterator(catalog.client(), sourceTable, PARTITION_BATCH_SIZE))
        .replica(Optional.of(replicaTable),
            Optional.of(new BufferedPartitionFetcher(catalog.client(), replicaTable, PARTITION_BATCH_SIZE)))
        .checksumFunction(checksumFunction)
        .build()
        .run();
    verify(diffListener, never()).onChangedTable(anyList());
    verify(diffListener, never()).onNewPartition(anyString(), any(Partition.class));
    verify(diffListener, never()).onChangedPartition(anyString(), any(Partition.class), anyList());
    verify(diffListener, never()).onDataChanged(anyString(), any(Partition.class));
  }

  @Test
  public void partitionDataHaveChanged() throws Exception {
    reset(checksumFunction);
    when(checksumFunction.apply(any(Path.class))).thenAnswer(new Answer<String>() {
      @Override
      public String answer(InvocationOnMock invocation) throws Throwable {
        Path path = (Path) invocation.getArguments()[0];
        if ("part=1".equals(path.getName())) {
          return "new part=1 checksum";
        }
        return path.getName();
      }
    });

    Table sourceTable = catalog.client().getTable(DATABASE, SOURCE_TABLE);
    Table replicaTable = catalog.client().getTable(DATABASE, REPLICA_TABLE);

    HiveDifferences
        .builder(diffListener)
        .comparatorRegistry(comparatorRegistry)
        .source(configuration, sourceTable, new PartitionIterator(catalog.client(), sourceTable, PARTITION_BATCH_SIZE))
        .replica(Optional.of(replicaTable),
            Optional.of(new BufferedPartitionFetcher(catalog.client(), replicaTable, PARTITION_BATCH_SIZE)))
        .checksumFunction(checksumFunction)
        .build()
        .run();
    verify(diffListener, never()).onChangedTable(anyList());
    verify(diffListener, never()).onNewPartition(anyString(), any(Partition.class));
    verify(diffListener, never()).onChangedPartition(anyString(), any(Partition.class), anyList());
    verify(diffListener, times(1))
        .onDataChanged("part=1", catalog.client().getPartition(DATABASE, SOURCE_TABLE, "part=1"));
  }

}
