package com.hotels.bdp.circustrain.core.client;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class DataManipulationClientFactoryManagerTest {

  private @Mock DataManipulationClientFactory clientFactory;

  private DataManipulationClientFactoryManager manager;
  private String sourceLocation;
  private String replicaLocation;
  private final String s3Path = "s3://<path>";
  private final String hdfsPath = "hdfs://<path>";

  @Before
  public void setup() {
    clientFactory = new TestDataManipulationClientFactory();
    List<DataManipulationClientFactory> clientFactories = Arrays.asList(clientFactory);
    manager = new DataManipulationClientFactoryManager(clientFactories);
    sourceLocation = s3Path;
    manager.withSourceLocation(new Path(sourceLocation));
  }

  @Test
  public void awsClientReturnedForS3S3Copy() {
    replicaLocation = s3Path;
    ((TestDataManipulationClientFactory) clientFactory).setS3Client();

    DataManipulationClient client = manager.getClientForPath(replicaLocation);
    Assert.assertTrue(client instanceof TestDataManipulationClient);
  }

  @Test
  public void awsMapReduceClientReturnedForS3S3Copy() {
    sourceLocation = hdfsPath;
    manager.withSourceLocation(new Path(sourceLocation));
    replicaLocation = s3Path;
    ((TestDataManipulationClientFactory) clientFactory).setS3MapreduceClient();

    DataManipulationClient client = manager.getClientForPath(replicaLocation);
    Assert.assertTrue(client instanceof TestDataManipulationClient);
  }

  @Test
  public void hdfsClientReturnedForS3S3Copy() {
    sourceLocation = hdfsPath;
    manager.withSourceLocation(new Path(sourceLocation));
    replicaLocation = hdfsPath;
    ((TestDataManipulationClientFactory) clientFactory).setHdfsClient();

    DataManipulationClient client = manager.getClientForPath(replicaLocation);
    Assert.assertTrue(client instanceof TestDataManipulationClient);
  }

  @Test(expected = UnsupportedOperationException.class)
  public void noSupportingFactory() {
    replicaLocation = "<path>";
    ((TestDataManipulationClientFactory) clientFactory).setS3Client();

    manager.getClientForPath(replicaLocation);
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
    public boolean supportsDeletion(String sourceLocation, String targetLocation) {
      System.out.println("Checking if supported");
      if (isS3ToS3(sourceLocation, targetLocation) && isS3S3Client) {
        return true;
      }
      if (isHdfsToS3(sourceLocation, targetLocation) && isS3MapreduceClient) {
        return true;
      }
      if (isHdfsToHdfs(sourceLocation, targetLocation) && isHdfsClient) {
        return true;
      }
      return false;
    }

    @Override
    public void withCopierOptions(Map<String, Object> copierOptions) {}

    private boolean isS3ToS3(String sourceLocation, String targetLocation) {
      return sourceLocation.toLowerCase().startsWith(S3_LOCATION)
          && targetLocation.toLowerCase().startsWith(S3_LOCATION);
    }

    private boolean isHdfsToS3(String sourceLocation, String targetLocation) {
      return sourceLocation.toLowerCase().startsWith(HDFS_LOCATION)
          && targetLocation.toLowerCase().startsWith(S3_LOCATION);
    }

    private boolean isHdfsToHdfs(String sourceLocation, String targetLocation) {
      return sourceLocation.toLowerCase().startsWith(HDFS_LOCATION)
          && targetLocation.toLowerCase().startsWith(HDFS_LOCATION);
    }

    public void setS3Client() {
      isS3S3Client = true;
    }

    public void setHdfsClient() {
      isHdfsClient = true;
    }

    public void setS3MapreduceClient() {
      isS3MapreduceClient = true;
    }
  }

  class TestDataManipulationClient implements DataManipulationClient {

    @Override
    public boolean delete(String path) throws IOException {
      return false;
    }
  }

}
