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
package com.hotels.bdp.circustrain.metrics.conf;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.hotels.bdp.circustrain.api.CircusTrainException;

public class GraphiteLoaderTest {

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  private final Configuration conf = new Configuration();

  private File clusterProperties;
  private Path path;
  private GraphiteLoader loader;

  @Before
  public void before() {
    clusterProperties = new File(temp.getRoot(), "cluster.properties");
    path = new Path(clusterProperties.toURI());
    loader = new GraphiteLoader(conf);
  }

  @Test
  public void readAllProps() throws IOException {
    Properties properties = new Properties();
    properties.put("graphite.host", "h");
    properties.put("graphite.prefix", "p");
    properties.put("graphite.namespace", "n");
    try (OutputStream outputStream = new FileOutputStream(clusterProperties)) {
      properties.store(outputStream, null);
    }

    Graphite graphite = loader.load(path);

    assertThat(graphite.getConfig(), is(path));
    assertThat(graphite.getHost(), is("h"));
    assertThat(graphite.getPrefix(), is("p"));
    assertThat(graphite.getNamespace(), is("n"));
  }

  @Test
  public void nullPath() throws IOException {
    Graphite graphite = loader.load(null);

    assertThat(graphite.getConfig(), is(nullValue()));
    assertThat(graphite.getHost(), is(nullValue()));
    assertThat(graphite.getPrefix(), is(nullValue()));
    assertThat(graphite.getNamespace(), is(nullValue()));
  }

  @Test(expected = CircusTrainException.class)
  public void pathDoesNotExist() throws IOException {
    loader.load(new Path(new File(temp.getRoot(), "dummy.properties").toURI()));
  }

  @SuppressWarnings("unchecked")
  @Test(expected = CircusTrainException.class)
  public void ioException() throws IOException {
    Path mockPath = mock(Path.class);
    when(mockPath.getFileSystem(conf)).thenThrow(IOException.class);

    loader.load(mockPath);
  }

}
