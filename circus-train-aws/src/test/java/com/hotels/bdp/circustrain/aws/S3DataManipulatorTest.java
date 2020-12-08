package com.hotels.bdp.circustrain.aws;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.fs.s3a.BasicAWSCredentialsProvider;
import org.gaul.s3proxy.junit.S3ProxyRule;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

@RunWith(MockitoJUnitRunner.class)
public class S3DataManipulatorTest {

  private static final String AWS_ACCESS_KEY = "access";
  private static final String AWS_SECRET_KEY = "secret";
  private static final String BUCKET = "bucket";
  private static final String EMPTY_BUCKET = "empty-bucket";
  private static final String PATH ="s3://" + BUCKET;
  private static final String EMPTY_PATH ="s3://" + EMPTY_BUCKET;

  public @Rule TemporaryFolder temp = new TemporaryFolder();
  public @Rule S3ProxyRule s3Proxy = S3ProxyRule.builder().withCredentials(AWS_ACCESS_KEY, AWS_SECRET_KEY).build();

  private S3DataManipulator s3DataManipulator;
  private AmazonS3 s3Client;

  @Before
  public void setUp() {
    s3Client = newClient();
    s3DataManipulator = new S3DataManipulator(s3Client);
  }

  private AmazonS3 newClient() {
    AwsClientBuilder.EndpointConfiguration endpointConfiguration = new AwsClientBuilder.EndpointConfiguration(s3Proxy.getUri().toString(),
        Regions.DEFAULT_REGION.getName());
    AmazonS3 newClient = AmazonS3ClientBuilder
        .standard()
        .withCredentials(new BasicAWSCredentialsProvider(AWS_ACCESS_KEY, AWS_SECRET_KEY))
        .withEndpointConfiguration(endpointConfiguration)
        .build();
    return newClient;
  }

  @Test
  public void deleteFails() {
    s3Client.createBucket(EMPTY_BUCKET);
    boolean result = s3DataManipulator.delete(EMPTY_PATH);
    assertThat(result, is(false));
  }

  @Test
  public void deleteSucceeds() throws IOException {
    s3Client.createBucket(BUCKET);
    File inputData = temp.newFile("data");

    s3Client.putObject(BUCKET, "key1/key2", inputData);
    boolean result = s3DataManipulator.delete(PATH);
    assertThat(result, is(true));
  }

}