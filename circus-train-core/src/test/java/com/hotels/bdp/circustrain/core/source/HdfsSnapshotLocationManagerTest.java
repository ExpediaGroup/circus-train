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
package com.hotels.bdp.circustrain.core.source;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import com.hotels.bdp.circustrain.api.CircusTrainException;
import com.hotels.bdp.circustrain.api.event.SourceCatalogListener;
import com.hotels.bdp.circustrain.core.source.HdfsSnapshotLocationManager.FileSystemFactory;

@RunWith(MockitoJUnitRunner.class)
public class HdfsSnapshotLocationManagerTest {

  private static final String EVENT_ID = "eventId";
  private static final String TABLE_LOCATION = "table_location";
  private static final String PARTITION_BASE_LOCATION = "partition_location";
  private static final String TABLE_NAME = "tableName";
  private static final String DB_NAME = "dbName";

  @Mock
  private FileSystemFactory fileSystemFactory;
  @Mock
  private FileSystem fileSystem;
  @Mock
  private SourceCatalogListener sourceCatalogListener;

  private final HiveConf hiveConf = new HiveConf();
  private Table sourceTable;
  private final Partition partition1 = new Partition();
  private final Partition partition2 = new Partition();
  private final List<Partition> partitions = Arrays.asList(partition1, partition2);

  @Before
  public void setupTable() {
    sourceTable = new Table();
    sourceTable.setDbName(DB_NAME);
    sourceTable.setTableName(TABLE_NAME);

    StorageDescriptor sd = new StorageDescriptor();
    sd.setLocation(TABLE_LOCATION);
    sourceTable.setSd(sd);
  }

  @Before
  public void setupFileSystem() throws IOException {
    when(fileSystemFactory.get(any(Path.class), any(Configuration.class))).thenReturn(fileSystem);
    when(fileSystem.makeQualified(any(Path.class))).thenAnswer(new Answer<Path>() {

      @Override
      public Path answer(InvocationOnMock invocation) throws Throwable {
        return (Path) invocation.getArguments()[0];
      }
    });
  }

  @Test
  public void getTableLocationUnpartitionedTableSnapshotsDisabled() throws Exception {
    HdfsSnapshotLocationManager manager = new HdfsSnapshotLocationManager(hiveConf, EVENT_ID, sourceTable, true,
        sourceCatalogListener);
    Path tableLocation = manager.getTableLocation();
    assertThat(tableLocation, is(new Path(TABLE_LOCATION)));
  }

  @Test
  public void getTableLocationPartitionedTableSnapshotsDisabled() throws Exception {
    StorageDescriptor sd = new StorageDescriptor();
    sd.setLocation(TABLE_LOCATION + "/partition1");
    partition1.setSd(sd);

    sd = new StorageDescriptor();
    sd.setLocation(TABLE_LOCATION + "/partition2");
    partition2.setSd(sd);

    HdfsSnapshotLocationManager manager = new HdfsSnapshotLocationManager(hiveConf, EVENT_ID, sourceTable, partitions,
        true, null, sourceCatalogListener);
    Path tableLocation = manager.getTableLocation();
    assertThat(tableLocation, is(new Path(TABLE_LOCATION)));
  }

  @Test
  public void getTableLocationPartitionedTableSnapshotsDisabledWithOverride() throws Exception {
    StorageDescriptor sd = new StorageDescriptor();
    sd.setLocation(PARTITION_BASE_LOCATION + "/partition1");
    partition1.setSd(sd);

    sd = new StorageDescriptor();
    sd.setLocation(PARTITION_BASE_LOCATION + "/partition2");
    partition2.setSd(sd);

    HdfsSnapshotLocationManager manager = new HdfsSnapshotLocationManager(hiveConf, EVENT_ID, sourceTable, partitions,
        true, PARTITION_BASE_LOCATION, sourceCatalogListener);
    Path tableLocation = manager.getTableLocation();
    assertThat(tableLocation, is(new Path(PARTITION_BASE_LOCATION)));
  }

  @Test
  public void getTableLocationUnpartitionedTableSnapshotsEnabled() throws Exception {
    when(fileSystem.exists(new Path(TABLE_LOCATION + "/.snapshot"))).thenReturn(true);
    when(fileSystem.createSnapshot(new Path(TABLE_LOCATION), EVENT_ID)).thenReturn(new Path("snapshotPath"));

    HdfsSnapshotLocationManager manager = new HdfsSnapshotLocationManager(hiveConf, EVENT_ID, sourceTable,
        Collections.<Partition> emptyList(), false, null, fileSystemFactory, sourceCatalogListener);

    verify(fileSystem).createSnapshot(new Path(TABLE_LOCATION), EVENT_ID);
    Path tableLocation = manager.getTableLocation();
    assertThat(tableLocation, is(new Path("snapshotPath")));
  }

  @Test
  public void getTableLocationPartitionedTableSnapshotsEnabled() throws Exception {
    when(fileSystem.exists(new Path(TABLE_LOCATION + "/.snapshot"))).thenReturn(true);
    when(fileSystem.createSnapshot(new Path(TABLE_LOCATION), EVENT_ID)).thenReturn(new Path("snapshotPath"));

    StorageDescriptor sd = new StorageDescriptor();
    sd.setLocation(TABLE_LOCATION + "/partition1");
    partition1.setSd(sd);

    sd = new StorageDescriptor();
    sd.setLocation(TABLE_LOCATION + "/partition2");
    partition2.setSd(sd);

    HdfsSnapshotLocationManager manager = new HdfsSnapshotLocationManager(hiveConf, EVENT_ID, sourceTable, partitions,
        false, null, fileSystemFactory, sourceCatalogListener);
    Path tableLocation = manager.getTableLocation();
    assertThat(tableLocation, is(new Path("snapshotPath")));
  }

  @Test
  public void getTableLocationPartitionedTableSnapshotsEnabledWithOverride() throws Exception {
    when(fileSystem.exists(new Path(PARTITION_BASE_LOCATION + "/.snapshot"))).thenReturn(true);
    when(fileSystem.createSnapshot(new Path(PARTITION_BASE_LOCATION), EVENT_ID)).thenReturn(new Path("snapshotPath"));

    StorageDescriptor sd = new StorageDescriptor();
    sd.setLocation(PARTITION_BASE_LOCATION + "/partition1");
    partition1.setSd(sd);

    sd = new StorageDescriptor();
    sd.setLocation(PARTITION_BASE_LOCATION + "/partition2");
    partition2.setSd(sd);

    HdfsSnapshotLocationManager manager = new HdfsSnapshotLocationManager(hiveConf, EVENT_ID, sourceTable, partitions,
        false, PARTITION_BASE_LOCATION, fileSystemFactory, sourceCatalogListener);
    Path tableLocation = manager.getTableLocation();
    assertThat(tableLocation, is(new Path("snapshotPath")));
  }

  @Test
  public void cleanUpDeletesSnapshotForUnpartitionedTable() throws IOException {
    when(fileSystem.exists(new Path(TABLE_LOCATION + "/.snapshot"))).thenReturn(true);
    when(fileSystem.createSnapshot(new Path(TABLE_LOCATION), EVENT_ID)).thenReturn(new Path("snapshotPath"));

    HdfsSnapshotLocationManager manager = new HdfsSnapshotLocationManager(hiveConf, EVENT_ID, sourceTable,
        Collections.<Partition> emptyList(), false, null, fileSystemFactory, sourceCatalogListener);
    manager.cleanUpLocations();
    verify(fileSystem).deleteSnapshot(new Path(TABLE_LOCATION), EVENT_ID);
  }

  @Test
  public void cleanUpDeletesSnapshotForPartitionedTableWithOverride() throws IOException {
    when(fileSystem.exists(new Path(PARTITION_BASE_LOCATION + "/.snapshot"))).thenReturn(true);
    when(fileSystem.createSnapshot(new Path(PARTITION_BASE_LOCATION), EVENT_ID)).thenReturn(new Path("snapshotPath"));

    StorageDescriptor sd = new StorageDescriptor();
    sd.setLocation(PARTITION_BASE_LOCATION + "/partition1");
    partition1.setSd(sd);

    sd = new StorageDescriptor();
    sd.setLocation(PARTITION_BASE_LOCATION + "/partition2");
    partition2.setSd(sd);

    HdfsSnapshotLocationManager manager = new HdfsSnapshotLocationManager(hiveConf, EVENT_ID, sourceTable, partitions,
        false, PARTITION_BASE_LOCATION, fileSystemFactory, sourceCatalogListener);
    manager.cleanUpLocations();
    verify(fileSystem).deleteSnapshot(new Path(PARTITION_BASE_LOCATION), EVENT_ID);
  }

  @Test
  public void calculateSubPaths() {
    StorageDescriptor sd = new StorageDescriptor();
    sd.setLocation("/a/partition1");
    partition1.setSd(sd);

    List<Path> subPaths = HdfsSnapshotLocationManager.calculateSubPaths(Collections.singletonList(partition1), "/a",
        "/b");
    assertThat(subPaths.get(0).toString(), is("/b/partition1"));
  }

  @Test
  public void calculateSubPathsFullUri() {
    StorageDescriptor sd = new StorageDescriptor();
    sd.setLocation("/a/partition1");
    partition1.setSd(sd);

    List<Path> subPaths = HdfsSnapshotLocationManager.calculateSubPaths(Collections.singletonList(partition1), "/a",
        "hdfs://someserver:8020/b");
    assertThat(subPaths.get(0).toString(), is("hdfs://someserver:8020/b/partition1"));
  }

  @Test
  public void calculateSubPathsUriEncodedPathAndPartition() {
    StorageDescriptor sd = new StorageDescriptor();
    sd.setLocation("hdfs://sandboxcluster/a%25b/partition1=url%25encoded.%3A");
    partition1.setSd(sd);

    List<Path> subPaths = HdfsSnapshotLocationManager.calculateSubPaths(Collections.singletonList(partition1),
        "hdfs://sandboxcluster/a%25b", "/b%25c");
    assertThat(subPaths.get(0).toString(), is("/b%25c/partition1=url%25encoded.%3A"));
  }

  @Test
  public void calculateSubPathsTrailingSlash() {
    StorageDescriptor sd = new StorageDescriptor();
    sd.setLocation("/a/partition1");
    partition1.setSd(sd);

    List<Path> subPaths = HdfsSnapshotLocationManager.calculateSubPaths(Collections.singletonList(partition1), "/a/",
        "/b");
    assertThat(subPaths.get(0).toString(), is("/b/partition1"));
  }

  @Test
  public void partitionSubPath() throws Exception {
    StorageDescriptor sd = new StorageDescriptor();
    sd.setLocation(PARTITION_BASE_LOCATION + "/partition1");
    partition1.setSd(sd);

    HdfsSnapshotLocationManager manager = new HdfsSnapshotLocationManager(hiveConf, EVENT_ID, sourceTable,
        Arrays.asList(partition1), false, PARTITION_BASE_LOCATION, fileSystemFactory, sourceCatalogListener);
    assertThat(manager.getPartitionSubPath(new Path(partition1.getSd().getLocation())), is(new Path("partition1")));
  }

  @Test
  public void partitionSubPathUriEncoded() throws Exception {
    Path path = new Path(PARTITION_BASE_LOCATION + "/partition%251");
    StorageDescriptor sd = new StorageDescriptor();
    sd.setLocation(path.toUri().getPath());
    partition1.setSd(sd);

    HdfsSnapshotLocationManager manager = new HdfsSnapshotLocationManager(hiveConf, EVENT_ID, sourceTable,
        Arrays.asList(partition1), false, PARTITION_BASE_LOCATION, fileSystemFactory, sourceCatalogListener);
    assertThat(manager.getPartitionSubPath(path), is(new Path("partition%251")));
  }

  @Test
  public void partitionSubPathWithSnapshot() throws Exception {
    when(fileSystem.exists(new Path(PARTITION_BASE_LOCATION + "/.snapshot"))).thenReturn(true);
    when(fileSystem.createSnapshot(new Path(PARTITION_BASE_LOCATION), EVENT_ID)).thenReturn(new Path("snapshotPath"));

    StorageDescriptor sd = new StorageDescriptor();
    sd.setLocation(PARTITION_BASE_LOCATION + "/partition1");
    partition1.setSd(sd);

    HdfsSnapshotLocationManager manager = new HdfsSnapshotLocationManager(hiveConf, EVENT_ID, sourceTable,
        Arrays.asList(partition1), false, PARTITION_BASE_LOCATION, fileSystemFactory, sourceCatalogListener);
    assertThat(manager.getPartitionSubPath(new Path(partition1.getSd().getLocation())), is(new Path("partition1")));
  }

  @Test(expected = CircusTrainException.class)
  public void invalidPartitionSubPath() throws Exception {
    StorageDescriptor sd = new StorageDescriptor();
    sd.setLocation("anotherBaseLocation" + "/partition1");
    partition1.setSd(sd);

    HdfsSnapshotLocationManager manager = new HdfsSnapshotLocationManager(hiveConf, EVENT_ID, sourceTable,
        Arrays.asList(partition1), false, PARTITION_BASE_LOCATION, fileSystemFactory, sourceCatalogListener);
    manager.getPartitionSubPath(new Path(partition1.getSd().getLocation()));
  }

}
