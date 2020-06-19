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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import static com.hotels.bdp.circustrain.core.data.DefaultDataManipulatorFactoryManager.DATA_MANIPULATOR_FACTORY_CLASS;
import static com.hotels.bdp.circustrain.core.data.DefaultDataManipulatorFactoryManagerTest.DataManipulatorType.HDFS;
import static com.hotels.bdp.circustrain.core.data.DefaultDataManipulatorFactoryManagerTest.DataManipulatorType.S3_MAPREDUCE;
import static com.hotels.bdp.circustrain.core.data.DefaultDataManipulatorFactoryManagerTest.DataManipulatorType.S3_S3;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import com.hotels.bdp.circustrain.api.data.DataManipulator;
import com.hotels.bdp.circustrain.api.data.DataManipulatorFactory;
import com.hotels.bdp.circustrain.api.data.DataManipulatorFactoryManager;

@RunWith(MockitoJUnitRunner.class)
public class DefaultDataManipulatorFactoryManagerTest {

  private DataManipulatorFactory s3s3DataManipulatorFactory = new TestDataManipulatorFactory(S3_S3);
  private DataManipulatorFactory s3MapReduceDataManipulatorFactory = new TestDataManipulatorFactory(S3_MAPREDUCE);
  private DataManipulatorFactory hdfsDataManipulatorFactory = new TestDataManipulatorFactory(HDFS);

  private DataManipulatorFactoryManager manager;
  private DataManipulatorFactory dataManipulatorFactory;
  private Path sourceLocation;
  private Path replicaLocation;
  private final String s3Path = "s3://<path>";
  private final String hdfsPath = "hdfs://<path>";
  private final Map<String, Object> copierOptions = new HashMap<>();

  @Before
  public void setup() {
    manager = new DefaultDataManipulatorFactoryManager(
        Arrays.asList(s3s3DataManipulatorFactory, s3MapReduceDataManipulatorFactory, hdfsDataManipulatorFactory));
    sourceLocation = new Path(s3Path);
  }

  @Test
  public void s3ManipulatorReturnedForS3S3Copy() {
    replicaLocation = new Path(s3Path);
    dataManipulatorFactory = manager.getFactory(sourceLocation, replicaLocation, copierOptions);

    assertEquals(S3_S3, ((TestDataManipulatorFactory) dataManipulatorFactory).getType());
  }

  @Test
  public void awsMapReduceManipulatorReturnedForHdfsS3Copy() {
    sourceLocation = new Path(hdfsPath);
    replicaLocation = new Path(s3Path);
    dataManipulatorFactory = manager.getFactory(sourceLocation, replicaLocation, copierOptions);

    assertEquals(S3_MAPREDUCE, ((TestDataManipulatorFactory) dataManipulatorFactory).getType());

  }

  @Test
  public void hdfsManipulatorReturnedForHdfsCopy() {
    sourceLocation = new Path(hdfsPath);
    replicaLocation = new Path(hdfsPath);
    dataManipulatorFactory = manager.getFactory(sourceLocation, replicaLocation, copierOptions);

    assertEquals(HDFS, ((TestDataManipulatorFactory) dataManipulatorFactory).getType());
  }

  @Test
  public void dataManipulatorReturnedFromCopierOption() {
    replicaLocation = new Path(hdfsPath);
    TestDataManipulatorFactory testFactory = new TestDataManipulatorFactory(HDFS);
    manager = new DefaultDataManipulatorFactoryManager(Arrays.asList(testFactory));
    copierOptions.put(DATA_MANIPULATOR_FACTORY_CLASS, testFactory.getClass().getName());

    dataManipulatorFactory = manager.getFactory(sourceLocation, replicaLocation, copierOptions);

    assertEquals(dataManipulatorFactory, testFactory);
  }

  @Test(expected = UnsupportedOperationException.class)
  public void noSupportingFactory() {
    replicaLocation = new Path("<path>");
    dataManipulatorFactory = manager.getFactory(sourceLocation, replicaLocation, copierOptions);

    assertTrue(((TestDataManipulatorFactory) dataManipulatorFactory).getType() == HDFS);
  }

  enum DataManipulatorType {
    S3_S3,
    S3_MAPREDUCE,
    HDFS
  }

  private class TestDataManipulatorFactory implements DataManipulatorFactory {

    private final String S3_LOCATION = "s3";
    private final String HDFS_LOCATION = "hdfs";
    
    private final DataManipulatorType dataManipulatorType;

    public TestDataManipulatorFactory(DataManipulatorType dataManipulatorType) {
      this.dataManipulatorType = dataManipulatorType;
    }

    @Override
    public DataManipulator newInstance(Path path, Map<String, Object> copierOptions) {
      return new TestDataManipulator();
    }

    @Override
    public boolean supportsSchemes(String sourceLocation, String targetLocation) {
      if (sourceLocation == null || targetLocation == null) {
        return false;
      }

      if (supportsS3ToS3(sourceLocation, targetLocation) && dataManipulatorType == S3_S3) {
        return true;
      }
      if (supportsHdfsToS3(sourceLocation, targetLocation) && dataManipulatorType == S3_MAPREDUCE) {
        return true;
      }
      if (supportsHdfsToHdfs(sourceLocation, targetLocation) && dataManipulatorType == HDFS) {
        return true;
      }
      return false;
    }

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

    public DataManipulatorType getType() {
      return dataManipulatorType;
    }
  }

  class TestDataManipulator implements DataManipulator {
    @Override
    public boolean delete(String path) throws IOException {
      return false;
    }
  }

}
