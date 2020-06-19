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

import static org.junit.Assert.assertTrue;

import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class HdfsDataManipulatorFactoryTest {

  private String sourceLocation;
  private String replicaLocation;

  private final String s3Path = "s3://<path>";
  private final String hdfsPath = "hdfs://<path>";

  private @Mock Configuration conf;

  private HdfsDataManipulatorFactory dataManipulatorFactory;

  @Before
  public void setup() {
    dataManipulatorFactory = new HdfsDataManipulatorFactory(conf);
  }

  // This manipulator factory will technically support all schemes including replicating to and from S3. However the S3
  // manipulator factories take higher precedence so this factory wont be used.
  @Test
  public void checkSupportsHdfs() {
    sourceLocation = hdfsPath;
    replicaLocation = hdfsPath;

    assertTrue(dataManipulatorFactory.supportsSchemes(sourceLocation, replicaLocation));
  }

  @Test
  public void checkSupportsHdfsUpperCase() {
    sourceLocation = hdfsPath.toUpperCase();
    replicaLocation = hdfsPath.toUpperCase();

    assertTrue(dataManipulatorFactory.supportsSchemes(sourceLocation, replicaLocation));
  }

  @Test
  public void checkSupportsS3ToHdfs() {
    sourceLocation = s3Path;
    replicaLocation = hdfsPath;

    assertTrue(dataManipulatorFactory.supportsSchemes(sourceLocation, replicaLocation));
  }

  @Test
  public void checkSupportsS3() {
    sourceLocation = s3Path;
    replicaLocation = s3Path;

    assertTrue(dataManipulatorFactory.supportsSchemes(sourceLocation, replicaLocation));
  }

  @Test
  public void checkSupportsHdfsToS3() {
    sourceLocation = hdfsPath;
    replicaLocation = s3Path;

    assertTrue(dataManipulatorFactory.supportsSchemes(sourceLocation, replicaLocation));
  }

  @Test
  public void checkSupportsRandomPaths() {
    sourceLocation = "<path>";
    replicaLocation = "<path>";

    assertTrue(dataManipulatorFactory.supportsSchemes(sourceLocation, replicaLocation));
  }
}
