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
  private static final String BUCKET_PATH = "s3://" + BUCKET;
  private static final String FOLDER = "folder";
  private static final String EMPTY_BUCKET = "empty-bucket";

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
  public void deleteFolderSucceeds() throws IOException {
    s3Client.createBucket(BUCKET);
    File inputData = temp.newFile("data");
    s3Client.putObject(BUCKET, FOLDER, inputData);
    boolean result = s3DataManipulator.delete(BUCKET_PATH + "/" + FOLDER);
    assertThat(result, is(true));
  }

  @Test
  public void deleteNonexistentFolderFails() {
    s3Client.createBucket(BUCKET);
    boolean result = s3DataManipulator.delete(BUCKET_PATH + "/nonexistent-folder");
    assertThat(result, is(false));
  }

  @Test
  public void deleteBucketFails() {
    s3Client.createBucket(BUCKET);
    boolean result = s3DataManipulator.delete(BUCKET_PATH);
    assertThat(result, is(false));
  }

  @Test
  public void deleteEmptyBucketFails() {
    s3Client.createBucket(EMPTY_BUCKET);
    boolean result = s3DataManipulator.delete("s3://" + EMPTY_BUCKET);
    assertThat(result, is(false));
  }
}
