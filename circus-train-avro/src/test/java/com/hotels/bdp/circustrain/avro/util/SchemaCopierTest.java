/**
 * Copyright (C) 2016-2020 Expedia, Inc.
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
package com.hotels.bdp.circustrain.avro.util;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.hotels.bdp.circustrain.api.copier.Copier;
import com.hotels.bdp.circustrain.api.copier.CopierContext;
import com.hotels.bdp.circustrain.api.copier.CopierFactory;
import com.hotels.bdp.circustrain.api.copier.CopierFactoryManager;
import com.hotels.bdp.circustrain.api.copier.CopierOptions;
import com.hotels.bdp.circustrain.api.event.EventTableReplication;
import com.hotels.bdp.circustrain.api.metrics.Metrics;

@RunWith(MockitoJUnitRunner.class)
public class SchemaCopierTest {

  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();
  private @Mock CopierFactoryManager copierFactoryManager;
  private @Mock CopierFactory copierFactory;
  private @Mock CopierOptions copierOptions;
  private @Mock Copier copier;
  private @Mock EventTableReplication eventTableReplication;
  private SchemaCopier schemaCopier;
  private final String eventId = "eventId";
  private @Mock Metrics metrics;

  @Before
  public void setUp() {
    schemaCopier = new SchemaCopier(new HiveConf(), copierFactoryManager, copierOptions);
  }

  @Test
  public void copiedToCorrectDestination() throws IOException {
    Path source = new Path(temporaryFolder.newFile("test.txt").toURI());
    File destination = temporaryFolder.newFolder();
    Path targetFile = new Path(destination.toString(), "test.txt");
    Map<String, Object> copierOptionsMap = new HashMap<>();
    copierOptionsMap.put(CopierOptions.COPY_DESTINATION_IS_FILE, "true");
    when(copierFactoryManager.getCopierFactory(eq(source), eq(targetFile), eq(copierOptionsMap)))
        .thenReturn(copierFactory);
    //when(copierFactory.newInstance(eq(eventId), eq(source), eq(targetFile), eq(copierOptionsMap))).thenReturn(copier);
    //TODO: looks like above specifically returns the mock copier only on certain input, need to do the same based on the context
    doReturn(copier).when(copierFactory.newInstance(any(CopierContext.class)));
    when(copier.copy()).thenReturn(metrics);
    when(metrics.getBytesReplicated()).thenReturn(123L);
    Path result = schemaCopier.copy(source.toString(), destination.toString(), eventTableReplication, eventId);
    assertThat(result, is(targetFile));
  }

  @Test(expected = NullPointerException.class)
  public void copyWithNullSourceParamThrowsException() throws IOException {
    File destination = temporaryFolder.newFolder();
    schemaCopier.copy(null, destination.toString(), eventTableReplication, eventId);
  }

  @Test(expected = IllegalArgumentException.class)
  public void copyWithEmptySourceParamThrowsException() throws IOException {
    File destination = temporaryFolder.newFolder();
    schemaCopier.copy("", destination.toString(), eventTableReplication, eventId);
  }

  @Test(expected = NullPointerException.class)
  public void copyWithNullDestinationParamThrowsException() throws IOException {
    File source = temporaryFolder.newFile("test.txt");
    schemaCopier.copy(source.toString(), null, eventTableReplication, eventId);
  }

  @Test(expected = IllegalArgumentException.class)
  public void copyWithEmptyDestinationParamThrowsException() throws IOException {
    File source = temporaryFolder.newFile("test.txt");
    schemaCopier.copy(source.toString(), "", eventTableReplication, eventId);
  }
}
