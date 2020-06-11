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
public class HdfsDataManipulationClientFactoryTest {

  private String sourceLocation;
  private String replicaLocation;

  private final String s3Path = "s3://<path>";
  private final String hdfsPath = "hdfs://<path>";

  private @Mock Configuration conf;

  private HdfsDataManipulationClientFactory clientFactory;

  @Before
  public void setup() {
    clientFactory = new HdfsDataManipulationClientFactory(conf);
  }

  // This client factory will technically support all schemes including replicating to and from s3. However the s3
  // client factories take higher precedence so this client factory wont be used for s3.
  @Test
  public void checkSupportsHdfs() {
    sourceLocation = hdfsPath;
    replicaLocation = hdfsPath;

    boolean support = clientFactory.supportsSchemes(sourceLocation, replicaLocation);
    assertTrue(support);
  }

  @Test
  public void checkSupportsHdfsUpperCase() {
    sourceLocation = hdfsPath.toUpperCase();
    replicaLocation = hdfsPath.toUpperCase();

    boolean support = clientFactory.supportsSchemes(sourceLocation, replicaLocation);
    assertTrue(support);
  }

  @Test
  public void checkSupportsS3ToHdfs() {
    sourceLocation = s3Path;
    replicaLocation = hdfsPath;

    boolean support = clientFactory.supportsSchemes(sourceLocation, replicaLocation);
    assertTrue(support);
  }

  @Test
  public void checkSupportsS3() {
    sourceLocation = s3Path;
    replicaLocation = s3Path;

    boolean support = clientFactory.supportsSchemes(sourceLocation, replicaLocation);
    assertTrue(support);
  }

  @Test
  public void checkSupportsHdfsToS3() {
    sourceLocation = hdfsPath;
    replicaLocation = s3Path;

    boolean support = clientFactory.supportsSchemes(sourceLocation, replicaLocation);
    assertTrue(support);
  }

  @Test
  public void checkSupportsRandomPaths() {
    sourceLocation = "<path>";
    replicaLocation = "<path>";

    boolean support = clientFactory.supportsSchemes(sourceLocation, replicaLocation);
    assertTrue(support);
  }
}
