/**
 * Copyright (C) 2016-2018 Expedia Inc and Apache Hadoop contributors.
 *
 * Based on {@code org.apache.hadoop.tools.mapred.lib.TestDynamicInputFormat} from Hadoop DistCp 2.7.1:
 *
 * https://github.com/apache/hadoop/blob/release-2.7.1/hadoop-tools/hadoop-distcp/src/test/java/org/apache/hadoop/tools/mapred/lib/TestDynamicInputFormat.java
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
package com.hotels.bdp.circustrain.s3mapreducecp.mapreduce.lib;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.DataOutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.apache.hadoop.security.Credentials;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.hotels.bdp.circustrain.s3mapreducecp.CopyListing;
import com.hotels.bdp.circustrain.s3mapreducecp.CopyListingFileStatus;
import com.hotels.bdp.circustrain.s3mapreducecp.S3MapReduceCpConstants;
import com.hotels.bdp.circustrain.s3mapreducecp.S3MapReduceCpOptions;
import com.hotels.bdp.circustrain.s3mapreducecp.StubContext;
import com.hotels.bdp.circustrain.s3mapreducecp.util.S3MapReduceCpTestUtils;

public class DynamicInputFormatTest {
  private static final Log LOG = LogFactory.getLog(DynamicInputFormatTest.class);

  private static final int N_FILES = 1000;
  private static final int NUM_SPLITS = 7;
  private static final Credentials CREDENTIALS = new Credentials();

  private MiniDFSCluster cluster;
  private List<String> expectedFilePaths = new ArrayList<>(N_FILES);

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Before
  public void setup() throws Exception {
    cluster = S3MapReduceCpTestUtils
        .newMiniClusterBuilder(getConfigurationForCluster())
        .numDataNodes(1)
        .format(true)
        .build();
    LOG.info("Cluster created, is cluster up: " + cluster.isClusterUp());
    for (int i = 0; i < N_FILES; ++i) {
      createFile(temporaryFolder.getRoot() + "/source/" + String.valueOf(i));
    }
  }

  @After
  public void destroy() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  private Configuration getConfigurationForCluster() {
    Configuration configuration = new Configuration();
    System.setProperty("test.build.data", "target/tmp/build/TEST_DYNAMIC_INPUT_FORMAT/data");
    configuration.set("hadoop.log.dir", "target/tmp");
    LOG.debug("fs.default.name  == " + configuration.get("fs.default.name"));
    LOG.debug("dfs.http.address == " + configuration.get("dfs.http.address"));
    return configuration;
  }

  private S3MapReduceCpOptions getOptions() throws Exception {
    Path source = new Path(cluster.getFileSystem().getUri().toString() + temporaryFolder.getRoot() + "/source");
    URI target = URI.create(cluster.getFileSystem().getUri().toString() + temporaryFolder.getRoot() + "/target/");
    return S3MapReduceCpOptions.builder(Arrays.asList(source), target).maxMaps(NUM_SPLITS).build();
  }

  private void createFile(String path) throws Exception {
    FileSystem fileSystem = null;
    DataOutputStream outputStream = null;
    try {
      fileSystem = cluster.getFileSystem();
      outputStream = fileSystem.create(new Path(path), true, 0);
      expectedFilePaths.add(fileSystem.listStatus(new Path(path))[0].getPath().toString());
    } finally {
      IOUtils.cleanup(null, fileSystem, outputStream);
    }
  }

  @Test
  public void getSplits() throws Exception {
    S3MapReduceCpOptions options = getOptions();
    Configuration configuration = new Configuration();
    configuration.set("mapred.map.tasks", String.valueOf(options.getMaxMaps()));
    CopyListing.getCopyListing(configuration, CREDENTIALS, options).buildListing(new Path(
        cluster.getFileSystem().getUri().toString() + temporaryFolder.getRoot() + "/testDynInputFormat/fileList.seq"),
        options);

    JobContext jobContext = new JobContextImpl(configuration, new JobID());
    DynamicInputFormat<Text, CopyListingFileStatus> inputFormat = new DynamicInputFormat<>();
    List<InputSplit> splits = inputFormat.getSplits(jobContext);

    int nFiles = 0;
    int taskId = 0;

    for (InputSplit split : splits) {
      RecordReader<Text, CopyListingFileStatus> recordReader = inputFormat.createRecordReader(split, null);
      StubContext stubContext = new StubContext(jobContext.getConfiguration(), recordReader, taskId);
      final TaskAttemptContext taskAttemptContext = stubContext.getContext();

      recordReader.initialize(splits.get(0), taskAttemptContext);
      float previousProgressValue = 0f;
      while (recordReader.nextKeyValue()) {
        CopyListingFileStatus fileStatus = recordReader.getCurrentValue();
        String source = fileStatus.getPath().toString();
        assertTrue(expectedFilePaths.contains(source));
        final float progress = recordReader.getProgress();
        assertTrue(progress >= previousProgressValue);
        assertTrue(progress >= 0.0f);
        assertTrue(progress <= 1.0f);
        previousProgressValue = progress;
        ++nFiles;
      }
      assertTrue(recordReader.getProgress() == 1.0f);

      ++taskId;
    }

    Assert.assertEquals(expectedFilePaths.size(), nFiles);
  }

  @Test
  public void getSplitRatio() throws Exception {
    assertEquals(1, DynamicInputFormat.getSplitRatio(1, 1000000000));
    assertEquals(2, DynamicInputFormat.getSplitRatio(11000000, 10));
    assertEquals(4, DynamicInputFormat.getSplitRatio(30, 700));
    assertEquals(2, DynamicInputFormat.getSplitRatio(30, 200));

    // Tests with negative value configuration
    Configuration conf = new Configuration();
    conf.setInt(S3MapReduceCpConstants.CONF_LABEL_MAX_CHUNKS_TOLERABLE, -1);
    conf.setInt(S3MapReduceCpConstants.CONF_LABEL_MAX_CHUNKS_IDEAL, -1);
    conf.setInt(S3MapReduceCpConstants.CONF_LABEL_MIN_RECORDS_PER_CHUNK, -1);
    conf.setInt(S3MapReduceCpConstants.CONF_LABEL_SPLIT_RATIO, -1);
    assertEquals(1, DynamicInputFormat.getSplitRatio(1, 1000000000, conf));
    assertEquals(2, DynamicInputFormat.getSplitRatio(11000000, 10, conf));
    assertEquals(4, DynamicInputFormat.getSplitRatio(30, 700, conf));
    assertEquals(2, DynamicInputFormat.getSplitRatio(30, 200, conf));

    // Tests with valid configuration
    conf.setInt(S3MapReduceCpConstants.CONF_LABEL_MAX_CHUNKS_TOLERABLE, 100);
    conf.setInt(S3MapReduceCpConstants.CONF_LABEL_MAX_CHUNKS_IDEAL, 30);
    conf.setInt(S3MapReduceCpConstants.CONF_LABEL_MIN_RECORDS_PER_CHUNK, 10);
    conf.setInt(S3MapReduceCpConstants.CONF_LABEL_SPLIT_RATIO, 53);
    assertEquals(53, DynamicInputFormat.getSplitRatio(3, 200, conf));
  }

}
