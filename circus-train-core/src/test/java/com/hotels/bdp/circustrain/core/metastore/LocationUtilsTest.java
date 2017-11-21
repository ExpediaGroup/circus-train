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
package com.hotels.bdp.circustrain.core.metastore;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

import static com.hotels.bdp.circustrain.core.metastore.HiveEntityFactory.newPartition;
import static com.hotels.bdp.circustrain.core.metastore.HiveEntityFactory.newStorageDescriptor;
import static com.hotels.bdp.circustrain.core.metastore.HiveEntityFactory.newTable;

import java.io.File;
import java.net.URI;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.collect.Lists;

@RunWith(MockitoJUnitRunner.class)
public class LocationUtilsTest {

  private @Mock Table table;
  private @Mock StorageDescriptor tableStorageDescriptor;
  private @Mock Partition partition;
  private @Mock StorageDescriptor partitionStorageDescriptor;

  @Before
  public void init() {
    when(table.getSd()).thenReturn(tableStorageDescriptor);
    when(partition.getSd()).thenReturn(partitionStorageDescriptor);
  }

  @Test
  public void tableHasLocation() throws Exception {
    when(tableStorageDescriptor.getLocation()).thenReturn("abc");
    assertThat(LocationUtils.hasLocation(table), is(true));
  }

  @Test
  public void tableHasEmptyLocation() throws Exception {
    when(tableStorageDescriptor.getLocation()).thenReturn("");
    assertThat(LocationUtils.hasLocation(table), is(false));
  }

  @Test
  public void tableHasNullLocation() throws Exception {
    assertThat(LocationUtils.hasLocation(table), is(false));
  }

  @Test
  public void partitionHasLocation() throws Exception {
    when(partitionStorageDescriptor.getLocation()).thenReturn("abc");
    assertThat(LocationUtils.hasLocation(partition), is(true));
  }

  @Test
  public void partitionHasEmptyLocation() throws Exception {
    when(partitionStorageDescriptor.getLocation()).thenReturn("");
    assertThat(LocationUtils.hasLocation(partition), is(false));
  }

  @Test
  public void partitionHasNullLocation() throws Exception {
    assertThat(LocationUtils.hasLocation(partition), is(false));
  }

  @Test
  public void locationAsUriTable() throws Exception {
    // Note the double encoding, you need to call uri.getPath() in order to get correct path string back
    Table table = createTable();
    assertThat(LocationUtils.locationAsUri(table), is(new URI("file:///a/b%2525c")));
    assertThat(LocationUtils.locationAsUri(table).getPath(), is("/a/b%25c"));
  }

  @Test
  public void locationAsUriPartition() throws Exception {
    // Note the double encoding, you need to call uri.getPath() in order to get correct path string back
    Partition partition = createPartition();
    assertThat(LocationUtils.locationAsUri(partition), is(new URI("file:///a/b%2525c/x=y")));
    assertThat(LocationUtils.locationAsUri(partition).getPath(), is("/a/b%25c/x=y"));
  }

  @Test
  public void locationAsPathTable() throws Exception {
    // Location is stored as url encoded string we need to get the same Path back
    assertThat(LocationUtils.locationAsPath(createTable()), is(new Path("file:///a/b%25c")));
  }

  @Test
  public void locationAsPathPartition() throws Exception {
    // Location is stored as url encoded string we need to get the same Path back
    assertThat(LocationUtils.locationAsPath(createPartition()), is(new Path("file:///a/b%25c/x=y")));
  }

  private Table createTable() {
    return newTable("name", "db", Lists.<FieldSchema> newArrayList(), newStorageDescriptor(new File("/a/b%c")));
  }

  private Partition createPartition() {
    Table table = createTable();
    Partition partition = newPartition(table);
    partition.setSd(newStorageDescriptor(new File("/a/b%c/x=y")));
    return partition;
  }
}
