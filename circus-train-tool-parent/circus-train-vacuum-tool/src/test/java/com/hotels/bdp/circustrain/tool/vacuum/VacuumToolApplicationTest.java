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
package com.hotels.bdp.circustrain.tool.vacuum;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
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

import com.hotels.bdp.circustrain.api.metastore.CloseableMetaStoreClient;
import com.hotels.bdp.circustrain.core.conf.ReplicaTable;
import com.hotels.bdp.circustrain.core.conf.TableReplication;
import com.hotels.bdp.circustrain.core.conf.TableReplications;
import com.hotels.bdp.circustrain.housekeeping.model.CircusTrainLegacyReplicaPath;
import com.hotels.housekeeping.model.LegacyReplicaPath;
import com.hotels.housekeeping.repository.LegacyReplicaPathRepository;
import com.hotels.housekeeping.service.HousekeepingService;

@RunWith(MockitoJUnitRunner.class)
public class VacuumToolApplicationTest {

  private static final String PARTITION_NAME = "partition=1/x=y";
  private static final String PARTITION_EVENT_1 = "ctp-20160728T110821.830Z-w5npK1yY";
  private static final String PARTITION_EVENT_2 = "ctp-20160728T110821.830Z-w5npK2yY";
  private static final String PARTITION_EVENT_3 = "ctp-20160728T110821.830Z-w5npK3yY";
  private static final String TABLE_EVENT_1 = "ctt-20160728T110821.830Z-w5npK1yY";
  private static final String UNPARTITIONED_TABLE_NAME = "unpartitioned_table";
  private static final String PARTITIONED_TABLE_NAME = "partitioned_table";
  private static final String DATABASE_NAME = "database";

  private HiveConf conf;
  private TableReplication partitionedReplication;
  private TableReplication unpartitionedReplication;
  private TableReplications replications;

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Mock
  private Supplier<CloseableMetaStoreClient> clientSupplier;
  @Mock
  private CloseableMetaStoreClient client;
  @Mock
  private Table unpartitionedTable;
  @Mock
  private StorageDescriptor unpartitionedSd;
  @Mock
  private Table partitionedTable;
  @Mock
  private StorageDescriptor partitionedSd;
  @Mock
  private Partition partition;
  @Mock
  private StorageDescriptor partitionSd;
  @Mock
  private LegacyReplicaPathRepository legacyReplicaPathRepository;
  @Mock
  private HousekeepingService housekeepingService;
  @Captor
  private ArgumentCaptor<LegacyReplicaPath> pathCaptor;

  private String unpartitionLocation;
  private String partitionedBaseLocation;
  private String partitionLocation1;
  private String partitionLocation2;
  private String partitionLocation3;

  @Before
  public void initialise() throws MetaException, NoSuchObjectException, TException, IOException {
    unpartitionLocation = new Path(temporaryFolder.newFolder("unpartitioned", TABLE_EVENT_1).toURI().toString())
        .toString();

    partitionedBaseLocation = PathUtils
        .normalise(new Path(temporaryFolder.newFolder("partitioned").toURI()))
        .toString();
    partitionLocation1 = PathUtils
        .normalise(new Path(temporaryFolder.newFolder("partitioned", PARTITION_EVENT_1, "yyyy", "mm", "dd").toURI()))
        .toString();
    partitionLocation2 = PathUtils
        .normalise(new Path(temporaryFolder.newFolder("partitioned", PARTITION_EVENT_2, "yyyy", "mm", "dd").toURI()))
        .toString();
    partitionLocation3 = PathUtils
        .normalise(new Path(temporaryFolder.newFolder("partitioned", PARTITION_EVENT_3, "yyyy", "mm", "dd").toURI()))
        .toString();

    conf = new HiveConf(new Configuration(false), VacuumToolApplicationTest.class);

    ReplicaTable partitionedReplicaTable = new ReplicaTable();
    partitionedReplicaTable.setDatabaseName(DATABASE_NAME);
    partitionedReplicaTable.setTableName(PARTITIONED_TABLE_NAME);

    partitionedReplication = new TableReplication();
    partitionedReplication.setReplicaTable(partitionedReplicaTable);

    ReplicaTable unpartitionedReplicaTable = new ReplicaTable();
    unpartitionedReplicaTable.setDatabaseName(DATABASE_NAME);
    unpartitionedReplicaTable.setTableName(UNPARTITIONED_TABLE_NAME);

    unpartitionedReplication = new TableReplication();
    unpartitionedReplication.setReplicaTable(unpartitionedReplicaTable);

    replications = new TableReplications();
    replications.setTableReplications(Arrays.asList(partitionedReplication, unpartitionedReplication));

    when(clientSupplier.get()).thenReturn(client);

    LegacyReplicaPath legacyReplicaPath = new CircusTrainLegacyReplicaPath("eventId", PARTITION_EVENT_1,
        partitionLocation1);
    when(legacyReplicaPathRepository.findAll()).thenReturn(Arrays.<LegacyReplicaPath> asList(legacyReplicaPath));

    when(unpartitionedTable.getDbName()).thenReturn(DATABASE_NAME);
    when(unpartitionedTable.getTableName()).thenReturn(UNPARTITIONED_TABLE_NAME);
    when(unpartitionedTable.getPartitionKeys()).thenReturn(Collections.<FieldSchema> emptyList());
    when(unpartitionedTable.getSd()).thenReturn(unpartitionedSd);
    when(unpartitionedSd.getLocation()).thenReturn(unpartitionLocation);
    when(partitionedTable.getDbName()).thenReturn(DATABASE_NAME);
    when(partitionedTable.getTableName()).thenReturn(PARTITIONED_TABLE_NAME);
    when(partitionedTable.getPartitionKeys())
        .thenReturn(Arrays.asList(new FieldSchema("local_date", "string", "comment")));
    when(partitionedTable.getSd()).thenReturn(partitionedSd);
    when(partitionedSd.getLocation()).thenReturn(partitionedBaseLocation);
    when(partition.getSd()).thenReturn(partitionSd);

    when(client.getTable(DATABASE_NAME, UNPARTITIONED_TABLE_NAME)).thenReturn(unpartitionedTable);
    when(client.getTable(DATABASE_NAME, PARTITIONED_TABLE_NAME)).thenReturn(partitionedTable);

    when(client.listPartitionNames(DATABASE_NAME, PARTITIONED_TABLE_NAME, (short) -1))
        .thenReturn(Arrays.asList(PARTITION_NAME));
  }

  @Test
  public void removePath() {
    VacuumToolApplication tool = new VacuumToolApplication(conf, clientSupplier, legacyReplicaPathRepository,
        housekeepingService, replications, false, (short) 100, 1000);
    tool.removePath(new Path(partitionLocation1));

    verify(housekeepingService).scheduleForHousekeeping(pathCaptor.capture());
    LegacyReplicaPath legacyReplicaPath = pathCaptor.getValue();
    assertThat(legacyReplicaPath.getPath(), is(partitionLocation1));
    assertThat(legacyReplicaPath.getPathEventId(), is(PARTITION_EVENT_1));
    assertThat(legacyReplicaPath.getEventId().startsWith("vacuum-"), is(true));
  }

  @Test
  public void fetchHousekeepingPaths() throws Exception {
    VacuumToolApplication tool = new VacuumToolApplication(conf, clientSupplier, legacyReplicaPathRepository,
        housekeepingService, replications, false, (short) 100, 1000);
    Set<Path> paths = tool.fetchHousekeepingPaths(legacyReplicaPathRepository);

    assertThat(paths.size(), is(1));
    assertThat(paths.iterator().next(), is(new Path(partitionLocation1)));
  }

  @Test
  public void run() throws Exception {
    // There are 3 paths on the FS: partitionLocation1, partitionLocation2, partitionLocation3
    replications.setTableReplications(Collections.singletonList(partitionedReplication));

    // The MS references path 1
    when(partitionSd.getLocation()).thenReturn(partitionLocation1);
    when(partition.getSd()).thenReturn(partitionSd);
    when(client.listPartitions(DATABASE_NAME, PARTITIONED_TABLE_NAME, (short) 1))
        .thenReturn(Collections.singletonList(partition));
    when(client.getPartitionsByNames(DATABASE_NAME, PARTITIONED_TABLE_NAME, Arrays.asList(PARTITION_NAME)))
        .thenReturn(Collections.singletonList(partition));

    // The HK references path 2
    when(legacyReplicaPathRepository.findAll())
        .thenReturn(Collections.singletonList(
            new CircusTrainLegacyReplicaPath("eventId", PARTITION_EVENT_2, partitionLocation2)));

    // So we expect path 3 to be scheduled for removal
    VacuumToolApplication tool = new VacuumToolApplication(conf, clientSupplier, legacyReplicaPathRepository,
        housekeepingService, replications, false, (short) 100, 1000);
    tool.run(null);

    verify(housekeepingService, times(1)).scheduleForHousekeeping(pathCaptor.capture());
    LegacyReplicaPath legacyReplicaPath = pathCaptor.getValue();
    assertThat(legacyReplicaPath.getPath(), is(partitionLocation3));
    assertThat(legacyReplicaPath.getPathEventId(), is(PARTITION_EVENT_3));
    assertThat(legacyReplicaPath.getEventId().startsWith("vacuum-"), is(true));
  }
}
