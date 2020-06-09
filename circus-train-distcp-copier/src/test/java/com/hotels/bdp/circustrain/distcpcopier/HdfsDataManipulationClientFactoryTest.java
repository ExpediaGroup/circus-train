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

import static org.junit.Assert.assertFalse;
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

  @Test
  public void checkSupportsHdfs() {
    sourceLocation = hdfsPath;
    replicaLocation = hdfsPath;

    boolean support = clientFactory.supportsDeletion(sourceLocation, replicaLocation);
    assertTrue(support);
  }

  @Test
  public void checkSupportsHdfsUpperCase() {
    sourceLocation = hdfsPath.toUpperCase();
    replicaLocation = hdfsPath.toUpperCase();

    boolean support = clientFactory.supportsDeletion(sourceLocation, replicaLocation);
    assertTrue(support);
  }

  @Test
  public void checkDoesntSupportS3() {
    sourceLocation = s3Path;
    replicaLocation = s3Path;

    boolean support = clientFactory.supportsDeletion(sourceLocation, replicaLocation);
    assertFalse(support);
  }

  @Test
  public void checkDoesntSupportS3ToHdfs() {
    sourceLocation = s3Path;
    replicaLocation = hdfsPath;

    boolean support = clientFactory.supportsDeletion(sourceLocation, replicaLocation);
    assertFalse(support);
  }

  @Test
  public void checkDoesntSupportHdfsToS3() {
    sourceLocation = hdfsPath;
    replicaLocation = s3Path;

    boolean support = clientFactory.supportsDeletion(sourceLocation, replicaLocation);
    assertFalse(support);
  }

  @Test
  public void checkDoesntSupportRandomPaths() {
    sourceLocation = "<path>";
    replicaLocation = "<path>";

    boolean support = clientFactory.supportsDeletion(sourceLocation, replicaLocation);
    assertFalse(support);
  }
}
