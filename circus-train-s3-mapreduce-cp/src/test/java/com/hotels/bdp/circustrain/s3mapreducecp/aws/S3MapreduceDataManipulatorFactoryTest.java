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
package com.hotels.bdp.circustrain.s3mapreducecp.aws;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class S3MapreduceDataManipulatorFactoryTest {

  private Path sourceLocation;
  private Path replicaLocation;

  private String sourceScheme;
  private String replicaScheme;

  private final Path s3Path = new Path("s3://<path>");
  private final Path hdfsPath = new Path("hdfs://<path>");

  private @Mock Configuration conf;

  private S3MapreduceDataManipulatorFactory dataManipulatorFactory;

  @Before
  public void setup() {
    dataManipulatorFactory = new S3MapreduceDataManipulatorFactory(conf);
  }

  @Test
  public void checkSupportsHdfsToS3() {
    sourceLocation = hdfsPath;
    replicaLocation = s3Path;
    sourceScheme = sourceLocation.toUri().getScheme();
    replicaScheme = replicaLocation.toUri().getScheme();

    assertTrue(dataManipulatorFactory.supportsSchemes(sourceScheme, replicaScheme));
  }

  @Test
  public void checkDoesntSupportS3() {
    sourceLocation = s3Path;
    replicaLocation = s3Path;
    sourceScheme = sourceLocation.toUri().getScheme();
    replicaScheme = replicaLocation.toUri().getScheme();

    assertFalse(dataManipulatorFactory.supportsSchemes(sourceScheme, replicaScheme));
  }

  @Test
  public void checkDoesntSupportHdfsToHdfs() {
    sourceLocation = hdfsPath;
    replicaLocation = hdfsPath;
    sourceScheme = sourceLocation.toUri().getScheme();
    replicaScheme = replicaLocation.toUri().getScheme();

    assertFalse(dataManipulatorFactory.supportsSchemes(sourceScheme, replicaScheme));
  }

  @Test
  public void checkDoesntSupportRandomPaths() {
    sourceLocation = new Path("<path>");
    replicaLocation = new Path("<path>");
    sourceScheme = sourceLocation.toUri().getScheme();
    replicaScheme = replicaLocation.toUri().getScheme();

    assertFalse(dataManipulatorFactory.supportsSchemes(sourceScheme, replicaScheme));
  }

}
