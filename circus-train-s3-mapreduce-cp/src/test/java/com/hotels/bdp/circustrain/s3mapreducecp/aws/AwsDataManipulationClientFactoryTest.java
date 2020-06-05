package com.hotels.bdp.circustrain.s3mapreducecp.aws;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class AwsDataManipulationClientFactoryTest {


  private String sourceLocation;
  private String replicaLocation;

  private final String s3Path = "s3://<path>";
  private final String hdfsPath = "hdfs://<path>";

  private @Mock Configuration conf;

  private AwsDataManipulationClientFactory clientFactory;

  @Before
  public void setup() {
    clientFactory = new AwsDataManipulationClientFactory(conf);
  }

  @Test
  public void checkSupportsHdfsToS3() {
    sourceLocation = hdfsPath;
    replicaLocation = s3Path;

    boolean support = clientFactory.supportsDeletion(sourceLocation, replicaLocation);
    assertTrue(support);
  }

  @Test
  public void checkSupportsHdfsToS3UpperCase() {
    sourceLocation = hdfsPath.toUpperCase();
    replicaLocation = s3Path.toUpperCase();

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
  public void checkDoesntSupportHdfsToHdfs() {
    sourceLocation = hdfsPath;
    replicaLocation = hdfsPath;

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
