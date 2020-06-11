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
package com.hotels.bdp.circustrain.core.data;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import com.hotels.bdp.circustrain.api.data.DataManipulationClient;
import com.hotels.bdp.circustrain.api.data.DataManipulationClientFactory;
import com.hotels.bdp.circustrain.api.data.DataManipulationClientFactoryManager;

@RunWith(MockitoJUnitRunner.class)
public class DefaultDataManipulationClientFactoryManagerTest {

  private DataManipulationClientFactory s3s3ClientFactory;
  private DataManipulationClientFactory s3MapReduceClientFactory;
  private DataManipulationClientFactory hdfsClientFactory;

  private DataManipulationClientFactoryManager manager;
  private DataManipulationClientFactory clientFactory;
  private Path sourceLocation;
  private Path replicaLocation;
  private final String s3Path = "s3://<path>";
  private final String hdfsPath = "hdfs://<path>";
  private final Map<String, Object> copierOptions = new HashMap<>();

  @Before
  public void setup() {
    s3s3ClientFactory = new TestDataManipulationClientFactory();
    ((TestDataManipulationClientFactory) s3s3ClientFactory).setS3Client();

    s3MapReduceClientFactory = new TestDataManipulationClientFactory();
    ((TestDataManipulationClientFactory) s3MapReduceClientFactory).setS3MapreduceClient();

    hdfsClientFactory = new TestDataManipulationClientFactory();
    ((TestDataManipulationClientFactory) hdfsClientFactory).setHdfsClient();

    manager = new DefaultDataManipulationClientFactoryManager(
        Arrays.asList(s3s3ClientFactory, s3MapReduceClientFactory, hdfsClientFactory));
    sourceLocation = new Path(s3Path);
  }

  @Test
  public void awsClientReturnedForS3S3Copy() {
    replicaLocation = new Path(s3Path);
    clientFactory = manager.getClientForPath(sourceLocation, replicaLocation, copierOptions);

    Assert.assertTrue(((TestDataManipulationClientFactory) clientFactory).isS3S3Client());
  }

  @Test
  public void awsMapReduceClientReturnedForS3S3Copy() {
    sourceLocation = new Path(hdfsPath);
    replicaLocation = new Path(s3Path);
    clientFactory = manager.getClientForPath(sourceLocation, replicaLocation, copierOptions);

    Assert.assertTrue(((TestDataManipulationClientFactory) clientFactory).isS3MapreduceClient());
  }

  @Test
  public void hdfsClientReturnedForS3S3Copy() {
    sourceLocation = new Path(hdfsPath);
    replicaLocation = new Path(hdfsPath);
    clientFactory = manager.getClientForPath(sourceLocation, replicaLocation, copierOptions);

    Assert.assertTrue(((TestDataManipulationClientFactory) clientFactory).isHdfsClient());
  }

  @Test(expected = UnsupportedOperationException.class)
  public void noSupportingFactory() {
    replicaLocation = new Path("<path>");
    clientFactory = manager.getClientForPath(sourceLocation, replicaLocation, copierOptions);

    Assert.assertTrue(((TestDataManipulationClientFactory) clientFactory).isHdfsClient());
  }

  class TestDataManipulationClientFactory implements DataManipulationClientFactory {

    private boolean isS3S3Client = false;
    private boolean isHdfsClient = false;
    private boolean isS3MapreduceClient = false;

    private final String S3_LOCATION = "s3";
    private final String HDFS_LOCATION = "hdfs";

    @Override
    public DataManipulationClient newInstance(String path) {
      return new TestDataManipulationClient();
    }

    @Override
    public boolean supportsSchemes(String sourceLocation, String targetLocation) {
      if (sourceLocation == null || targetLocation == null) {
        return false;
      }

      if (supportsS3ToS3(sourceLocation, targetLocation) && isS3S3Client) {
        return true;
      }
      if (supportsHdfsToS3(sourceLocation, targetLocation) && isS3MapreduceClient) {
        return true;
      }
      if (supportsHdfsToHdfs(sourceLocation, targetLocation) && isHdfsClient) {
        return true;
      }
      return false;
    }

    @Override
    public void withCopierOptions(Map<String, Object> copierOptions) {}

    private boolean supportsS3ToS3(String sourceLocation, String targetLocation) {
      return sourceLocation.toLowerCase().startsWith(S3_LOCATION)
          && targetLocation.toLowerCase().startsWith(S3_LOCATION);
    }

    private boolean supportsHdfsToS3(String sourceLocation, String targetLocation) {
      return sourceLocation.toLowerCase().startsWith(HDFS_LOCATION)
          && targetLocation.toLowerCase().startsWith(S3_LOCATION);
    }

    private boolean supportsHdfsToHdfs(String sourceLocation, String targetLocation) {
      // supports all replication
      return true;
    }

    public void setS3Client() {
      isS3S3Client = true;
    }

    public void setS3MapreduceClient() {
      isS3MapreduceClient = true;
    }

    public void setHdfsClient() {
      isHdfsClient = true;
    }

    public boolean isS3S3Client() {
      return isS3S3Client;
    }

    public boolean isS3MapreduceClient() {
      return isS3MapreduceClient;
    }

    public boolean isHdfsClient() {
      return isHdfsClient;
    }
  }

  class TestDataManipulationClient implements DataManipulationClient {

    @Override
    public boolean delete(String path) throws IOException {
      return false;
    }
  }

}
