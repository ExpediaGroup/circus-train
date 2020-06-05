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
