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
package com.hotels.bdp.circustrain.distcpcopier;

import static java.nio.charset.StandardCharsets.UTF_8;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.codahale.metrics.MetricRegistry;
import com.google.common.io.Files;

import com.hotels.bdp.circustrain.api.CircusTrainException;
import com.hotels.bdp.circustrain.api.metrics.Metrics;

public class DistCpCopierTest {

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  private final Configuration conf = new Configuration();
  private final MetricRegistry registry = new MetricRegistry();
  private DistCpCopier copier;
  private Path sourceDataBaseLocation;
  private List<Path> sourceDataLocations;
  private Path replicaDataLocation;

  @Before
  public void init() throws Exception {
    File input = temp.newFolder("input");
    File inputSub2 = new File(input, "sub1/sub2");
    inputSub2.mkdirs();
    Files.asCharSink(new File(inputSub2, "data"), UTF_8).write("test1");
    File inputSub4 = new File(input, "sub3/sub4");
    inputSub4.mkdirs();
    Files.asCharSink(new File(inputSub4, "data"), UTF_8).write("test2");

    File output = temp.newFolder("output");

    sourceDataBaseLocation = new Path(input.toURI());
    sourceDataLocations = new ArrayList<>();
    sourceDataLocations.add(new Path(inputSub2.toURI()));
    sourceDataLocations.add(new Path(inputSub4.toURI()));
    replicaDataLocation = new Path(output.toURI());
  }

  @Test
  public void typical() throws Exception {
    copier = new DistCpCopier(conf, sourceDataBaseLocation, sourceDataLocations, replicaDataLocation, null, registry);

    Metrics metrics = copier.copy();
    assertThat(metrics, not(nullValue()));

    String outputPath = replicaDataLocation.toUri().getPath();

    File outputSub2Data = new File(outputPath, "sub1/sub2/data");
    assertTrue(outputSub2Data.exists());
    assertThat(Files.asCharSource(outputSub2Data, UTF_8).read(), is("test1"));

    File outputSub4Data = new File(outputPath, "sub3/sub4/data");
    assertTrue(outputSub4Data.exists());
    assertThat(Files.asCharSource(outputSub4Data, UTF_8).read(), is("test2"));
    assertThat(registry.getGauges().containsKey(RunningMetrics.DIST_CP_BYTES_REPLICATED.name()), is(true));
  }

  @Test
  public void typicalOneFile() throws Exception {
    Path inputFile = new Path(sourceDataBaseLocation, "sub1/sub2/data");
    Path targetFile = new Path(replicaDataLocation, "output.txt");
    copier = new DistCpCopier(conf, inputFile, Collections.<Path>emptyList(), targetFile, null, registry);

    Metrics metrics = copier.copy();
    assertThat(metrics, not(nullValue()));

    String outputPath = targetFile.toUri().getPath();

    Path parent = targetFile.getParent();
    FileSystem fs = parent.getFileSystem(conf);
    int fileCopyCount = fs.listStatus(parent).length;
    assertThat(fileCopyCount, is(1));
    File outputSub2Data = new File(outputPath);
    assertTrue(outputSub2Data.exists());
    assertThat(Files.asCharSource(outputSub2Data, UTF_8).read(), is("test1"));
  }

  @Test
  public void cleanUpOnFailure() throws Exception {
    Map<String, Object> copierOptions = new HashMap<>();
    copierOptions.put("file-attribute", "xattr"); // This will cause the copier to fail after having copied the data
    copier = new DistCpCopier(conf, sourceDataBaseLocation, sourceDataLocations, replicaDataLocation, copierOptions,
        registry);

    try {
      copier.copy();
      fail("Expecting copier failure");
    } catch (CircusTrainException e) {
      String outputPath = replicaDataLocation.toUri().toString();
      assertFalse(new File(outputPath).exists());
    }
  }

}
