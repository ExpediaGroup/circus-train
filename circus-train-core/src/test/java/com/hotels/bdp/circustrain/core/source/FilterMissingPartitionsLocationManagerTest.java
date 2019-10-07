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
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.collect.Lists;

import com.hotels.bdp.circustrain.api.SourceLocationManager;

@RunWith(MockitoJUnitRunner.class)
public class FilterMissingPartitionsLocationManagerTest {

  @Mock
  private SourceLocationManager sourceLocationManager;
  @Mock
  private HiveConf hiveConf;
  @Mock
  private Path path;
  @Mock
  private Path missingPath;
  @Mock
  private Path exceptionThrowingPath;
  @Mock
  private FileSystem fileSystem;
  private FilterMissingPartitionsLocationManager filterMissingPartitionsLocationManager;

  @Before
  public void setUp() throws IOException {
    filterMissingPartitionsLocationManager = new FilterMissingPartitionsLocationManager(sourceLocationManager,
        hiveConf);
    when(path.getFileSystem(hiveConf)).thenReturn(fileSystem);
    when(fileSystem.exists(path)).thenReturn(true);
    when(fileSystem.exists(missingPath)).thenReturn(false);
    when(fileSystem.exists(exceptionThrowingPath)).thenThrow(new IOException());
  }

  @Test
  public void getPartitionLocations() {
    List<Path> paths = Lists.newArrayList(path, missingPath);
    when(sourceLocationManager.getPartitionLocations()).thenReturn(paths);

    List<Path> filteredPaths = filterMissingPartitionsLocationManager.getPartitionLocations();
    List<Path> expected = Lists.newArrayList(path);
    assertThat(filteredPaths, is(expected));
  }

  @Test
  public void getPartitionLocationsExceptionThrowingPathIsSkipped() {
    List<Path> paths = Lists.newArrayList(path, exceptionThrowingPath);
    when(sourceLocationManager.getPartitionLocations()).thenReturn(paths);

    List<Path> filteredPaths = filterMissingPartitionsLocationManager.getPartitionLocations();
    List<Path> expected = Lists.newArrayList(path);
    assertThat(filteredPaths, is(expected));
  }

  @Test
  public void getTableLocation() {
    filterMissingPartitionsLocationManager.getTableLocation();
    verify(sourceLocationManager).getTableLocation();
  }

  @Test
  public void cleanUpLocations() {
    filterMissingPartitionsLocationManager.cleanUpLocations();
    verify(sourceLocationManager).cleanUpLocations();
  }

  @Test
  public void getPartitionSubPath() {
    Path partitionLocation = new Path("/tmp");
    filterMissingPartitionsLocationManager.getPartitionSubPath(partitionLocation);
    verify(sourceLocationManager).getPartitionSubPath(partitionLocation);
  }

}
