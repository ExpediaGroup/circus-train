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
package com.hotels.bdp.circustrain.s3s3copier.aws;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class AwsDataManipulationClientFactoryTest {

  private Path sourceLocation;
  private Path replicaLocation;

  private String sourceScheme;
  private String replicaScheme;

  private final String s3Path = "s3://<path>";
  private final String hdfsPath = "hdfs://<path>";

  private @Mock Configuration conf;
  private @Mock AmazonS3ClientFactory s3ClientFactory;

  private AwsS3DataManipulationClientFactory clientFactory;

  @Before
  public void setup() {
    clientFactory = new AwsS3DataManipulationClientFactory(s3ClientFactory);
  }

  @Test
  public void checkSupportsS3ToS3() {
    sourceLocation = new Path(s3Path);
    replicaLocation = new Path(s3Path);
    sourceScheme = sourceLocation.toUri().getScheme();
    replicaScheme = replicaLocation.toUri().getScheme();

    boolean support = clientFactory.supportsSchemes(sourceScheme, replicaScheme);
    assertTrue(support);
  }

  @Test
  public void checkSupportsHdfsToS3UpperCase() {
    sourceLocation = new Path(s3Path.toUpperCase());
    replicaLocation = new Path(s3Path.toUpperCase());
    sourceScheme = sourceLocation.toUri().getScheme();
    replicaScheme = replicaLocation.toUri().getScheme();

    boolean support = clientFactory.supportsSchemes(sourceScheme, replicaScheme);

    assertTrue(support);
  }

  @Test
  public void checkDoesntSupportHdfs() {
    sourceLocation = new Path(hdfsPath);
    replicaLocation = new Path(hdfsPath);
    sourceScheme = sourceLocation.toUri().getScheme();
    replicaScheme = replicaLocation.toUri().getScheme();

    boolean support = clientFactory.supportsSchemes(sourceScheme, replicaScheme);
    assertFalse(support);
  }

  @Test
  public void checkDoesntSupportHdfsToS3() {
    sourceLocation = new Path(hdfsPath);
    replicaLocation = new Path(s3Path);
    sourceScheme = sourceLocation.toUri().getScheme();
    replicaScheme = replicaLocation.toUri().getScheme();

    boolean support = clientFactory.supportsSchemes(sourceScheme, replicaScheme);
    assertFalse(support);
  }
  
  @Test
  public void checkDoesntSupportS3ToHdfs() {
    sourceLocation = new Path(s3Path);
    replicaLocation = new Path(hdfsPath);
    sourceScheme = sourceLocation.toUri().getScheme();
    replicaScheme = replicaLocation.toUri().getScheme();

    boolean support = clientFactory.supportsSchemes(sourceScheme, replicaScheme);
    assertFalse(support);
  }

  @Test
  public void checkDoesntSupportRandomPaths() {
    sourceLocation = new Path("<path>");
    replicaLocation = new Path("<path>");

    sourceScheme = sourceLocation.toUri().getScheme();
    replicaScheme = replicaLocation.toUri().getScheme();

    boolean support = clientFactory.supportsSchemes(sourceScheme, replicaScheme);
    assertFalse(support);
  }

}
