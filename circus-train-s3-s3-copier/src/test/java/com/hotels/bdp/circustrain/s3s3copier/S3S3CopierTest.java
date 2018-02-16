/**
 * Copyright (C) 2016-2018 Expedia Inc.
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
package com.hotels.bdp.circustrain.s3s3copier;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.BasicAWSCredentialsProvider;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.transfer.Copy;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferProgress;
import com.amazonaws.services.s3.transfer.internal.TransferStateChangeListener;
import com.amazonaws.util.IOUtils;
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.io.Files;

import com.hotels.bdp.circustrain.api.CircusTrainException;
import com.hotels.bdp.circustrain.api.metrics.Metrics;
import com.hotels.bdp.circustrain.common.test.junit.rules.S3ProxyRule;
import com.hotels.bdp.circustrain.s3s3copier.aws.AmazonS3ClientFactory;
import com.hotels.bdp.circustrain.s3s3copier.aws.ListObjectsRequestFactory;
import com.hotels.bdp.circustrain.s3s3copier.aws.TransferManagerFactory;

@RunWith(MockitoJUnitRunner.class)
public class S3S3CopierTest {

  private static final String AWS_ACCESS_KEY = "access";
  private static final String AWS_SECRET_KEY = "secret";

  public @Rule TemporaryFolder temp = new TemporaryFolder();
  public @Rule S3ProxyRule s3Proxy = S3ProxyRule.builder().withCredentials(AWS_ACCESS_KEY, AWS_SECRET_KEY).build();
  private final ListObjectsRequestFactory listObjectsRequestFactory = new ListObjectsRequestFactory();
  private final TransferManagerFactory transferManagerFactory = new TransferManagerFactory();
  private final MetricRegistry registry = new MetricRegistry();

  private final S3S3CopierOptions s3S3CopierOptions = new S3S3CopierOptions(new HashMap<String, Object>());

  private @Mock AmazonS3ClientFactory s3ClientFactory;

  private File inputData;

  private AmazonS3 client;

  @Before
  public void setUp() throws Exception {
    inputData = temp.newFile("data");
    Files.write("bar foo", inputData, Charsets.UTF_8);

    client = newClient();
    client.createBucket("source");
    client.createBucket("target");

    when(s3ClientFactory.newInstance(any(AmazonS3URI.class), any(S3S3CopierOptions.class))).thenReturn(newClient());
  }

  private AmazonS3 newClient() {
    EndpointConfiguration endpointConfiguration = new EndpointConfiguration(s3Proxy.getProxyUrl(),
        Regions.DEFAULT_REGION.getName());
    AmazonS3 newClient = AmazonS3ClientBuilder
        .standard()
        .withCredentials(new BasicAWSCredentialsProvider(AWS_ACCESS_KEY, AWS_SECRET_KEY))
        .withEndpointConfiguration(endpointConfiguration)
        .build();
    return newClient;
  }

  @Test
  public void copyOneObject() throws Exception {
    client.putObject("source", "data", inputData);

    Path sourceBaseLocation = new Path("s3://source/");
    Path replicaLocation = new Path("s3://target/");
    List<Path> sourceSubLocations = new ArrayList<>();
    S3S3Copier s3s3Copier = newS3S3Copier(sourceBaseLocation, sourceSubLocations, replicaLocation);
    Metrics metrics = s3s3Copier.copy();
    assertThat(metrics.getBytesReplicated(), is(7L));
    assertThat(metrics.getMetrics().get(S3S3CopierMetrics.Metrics.TOTAL_BYTES_TO_REPLICATE.name()), is(7L));
    S3Object object = client.getObject("target", "data");
    String data = IOUtils.toString(object.getObjectContent());
    assertThat(data, is("bar foo"));
    assertThat(registry.getGauges().containsKey(RunningMetrics.S3S3_CP_BYTES_REPLICATED.name()), is(true));
  }

  private S3S3Copier newS3S3Copier(Path sourceBaseLocation, List<Path> sourceSubLocations, Path replicaLocation) {
    return new S3S3Copier(sourceBaseLocation, sourceSubLocations, replicaLocation, s3ClientFactory,
        transferManagerFactory, listObjectsRequestFactory, registry, s3S3CopierOptions);
  }

  @Test
  public void copyOneObjectUsingKeys() throws Exception {
    client.putObject("source", "bar/data", inputData);

    Path sourceBaseLocation = new Path("s3://source/bar/");
    Path replicaLocation = new Path("s3://target/foo/");
    List<Path> sourceSubLocations = new ArrayList<>();
    S3S3Copier s3s3Copier = newS3S3Copier(sourceBaseLocation, sourceSubLocations, replicaLocation);
    s3s3Copier.copy();
    S3Object object = client.getObject("target", "foo/data");
    String data = IOUtils.toString(object.getObjectContent());
    assertThat(data, is("bar foo"));
  }

  @Test
  public void copyOneObjectPartitioned() throws Exception {
    client.putObject("source", "year=2016/data", inputData);

    Path sourceBaseLocation = new Path("s3://source/");
    Path replicaLocation = new Path("s3://target/foo/");
    List<Path> sourceSubLocations = Lists.newArrayList(new Path(sourceBaseLocation, "year=2016"));
    S3S3Copier s3s3Copier = newS3S3Copier(sourceBaseLocation, sourceSubLocations, replicaLocation);
    Metrics metrics = s3s3Copier.copy();
    assertThat(metrics.getBytesReplicated(), is(7L));
    S3Object object = client.getObject("target", "foo/year=2016/data");
    String data = IOUtils.toString(object.getObjectContent());
    assertThat(data, is("bar foo"));
  }

  @Test
  public void copyOneObjectPartitionedSourceBaseNested() throws Exception {
    client.putObject("source", "nested/year=2016/data", inputData);

    Path sourceBaseLocation = new Path("s3://source/nested");// no slash at the end
    Path replicaLocation = new Path("s3://target/foo/");
    List<Path> sourceSubLocations = Lists.newArrayList(new Path(sourceBaseLocation, "year=2016"));
    S3S3Copier s3s3Copier = newS3S3Copier(sourceBaseLocation, sourceSubLocations, replicaLocation);
    s3s3Copier.copy();
    S3Object object = client.getObject("target", "foo/year=2016/data");
    String data = IOUtils.toString(object.getObjectContent());
    assertThat(data, is("bar foo"));
  }

  @Test
  public void copyOneObjectPartitionedHandlingS3ASchemes() throws Exception {
    client.putObject("source", "year=2016/data", inputData);

    Path sourceBaseLocation = new Path("s3a://source/");
    Path replicaLocation = new Path("s3a://target/foo/");
    List<Path> sourceSubLocations = Lists.newArrayList(new Path(sourceBaseLocation, "year=2016"));
    S3S3Copier s3s3Copier = newS3S3Copier(sourceBaseLocation, sourceSubLocations, replicaLocation);
    s3s3Copier.copy();
    S3Object object = client.getObject("target", "foo/year=2016/data");
    String data = IOUtils.toString(object.getObjectContent());
    assertThat(data, is("bar foo"));
  }

  @Test
  public void copyMultipleObjects() throws Exception {
    // Making sure we only request 1 file at the time so we need to loop
    ListObjectsRequestFactory mockListObjectRequestFactory = Mockito.mock(ListObjectsRequestFactory.class);
    when(mockListObjectRequestFactory.newInstance()).thenReturn(new ListObjectsRequest().withMaxKeys(1));

    client.putObject("source", "bar/data1", inputData);
    client.putObject("source", "bar/data2", inputData);

    Path sourceBaseLocation = new Path("s3://source/bar/");
    Path replicaLocation = new Path("s3://target/foo/");
    List<Path> sourceSubLocations = new ArrayList<>();
    S3S3Copier s3s3Copier = new S3S3Copier(sourceBaseLocation, sourceSubLocations, replicaLocation, s3ClientFactory,
        transferManagerFactory, mockListObjectRequestFactory, registry, s3S3CopierOptions);
    Metrics metrics = s3s3Copier.copy();
    assertThat(metrics.getBytesReplicated(), is(14L));

    S3Object object1 = client.getObject("target", "foo/data1");
    String data1 = IOUtils.toString(object1.getObjectContent());
    assertThat(data1, is("bar foo"));
    S3Object object2 = client.getObject("target", "foo/data2");
    String data2 = IOUtils.toString(object2.getObjectContent());
    assertThat(data2, is("bar foo"));
  }

  @Test
  public void copyCheckTransferManagerIsShutdown() throws Exception {
    client.putObject("source", "data", inputData);
    Path sourceBaseLocation = new Path("s3://source/");
    Path replicaLocation = new Path("s3://target/");
    List<Path> sourceSubLocations = new ArrayList<>();

    TransferManagerFactory mockedTransferManagerFactory = Mockito.mock(TransferManagerFactory.class);
    TransferManager mockedTransferManager = Mockito.mock(TransferManager.class);
    when(mockedTransferManagerFactory.newInstance(any(AmazonS3.class), eq(s3S3CopierOptions)))
        .thenReturn(mockedTransferManager);
    Copy copy = Mockito.mock(Copy.class);
    when(mockedTransferManager.copy(any(CopyObjectRequest.class), any(AmazonS3.class),
        any(TransferStateChangeListener.class))).thenReturn(copy);
    TransferProgress transferProgress = new TransferProgress();
    when(copy.getProgress()).thenReturn(transferProgress);
    S3S3Copier s3s3Copier = new S3S3Copier(sourceBaseLocation, sourceSubLocations, replicaLocation, s3ClientFactory,
        mockedTransferManagerFactory, listObjectsRequestFactory, registry, s3S3CopierOptions);
    s3s3Copier.copy();
    verify(mockedTransferManager).shutdownNow();
  }

  @Test
  public void copySafelyShutDownTransferManagerWhenNotInitialised() throws Exception {
    Path sourceBaseLocation = new Path("s3://source/");
    Path replicaLocation = new Path("s3://target/");
    List<Path> sourceSubLocations = new ArrayList<>();
    TransferManagerFactory mockedTransferManagerFactory = Mockito.mock(TransferManagerFactory.class);
    when(mockedTransferManagerFactory.newInstance(any(AmazonS3.class), eq(s3S3CopierOptions)))
        .thenThrow(new RuntimeException("error in instance"));
    S3S3Copier s3s3Copier = new S3S3Copier(sourceBaseLocation, sourceSubLocations, replicaLocation, s3ClientFactory,
        mockedTransferManagerFactory, listObjectsRequestFactory, registry, s3S3CopierOptions);
    try {
      s3s3Copier.copy();
    } catch (RuntimeException e) {
      assertThat(e.getMessage(), is("error in instance"));
    }
  }

  @Test
  public void copyCheckTransferManagerIsShutdownWhenSubmittingJobExceptionsAreThrown() throws Exception {
    client.putObject("source", "data", inputData);
    Path sourceBaseLocation = new Path("s3://source/");
    Path replicaLocation = new Path("s3://target/");
    List<Path> sourceSubLocations = new ArrayList<>();

    TransferManagerFactory mockedTransferManagerFactory = Mockito.mock(TransferManagerFactory.class);
    TransferManager mockedTransferManager = Mockito.mock(TransferManager.class);
    when(mockedTransferManagerFactory.newInstance(any(AmazonS3.class), eq(s3S3CopierOptions)))
        .thenReturn(mockedTransferManager);
    when(mockedTransferManager.copy(any(CopyObjectRequest.class), any(AmazonS3.class),
        any(TransferStateChangeListener.class))).thenThrow(new AmazonServiceException("MyCause"));
    S3S3Copier s3s3Copier = new S3S3Copier(sourceBaseLocation, sourceSubLocations, replicaLocation, s3ClientFactory,
        mockedTransferManagerFactory, listObjectsRequestFactory, registry, s3S3CopierOptions);
    try {
      s3s3Copier.copy();
      fail("exception should have been thrown");
    } catch (CircusTrainException e) {
      verify(mockedTransferManager).shutdownNow();
      assertThat(e.getCause().getMessage(), startsWith("MyCause"));
    }
  }

  @Test
  public void copyCheckTransferManagerIsShutdownWhenCopyExceptionsAreThrown() throws Exception {
    client.putObject("source", "data", inputData);
    Path sourceBaseLocation = new Path("s3://source/");
    Path replicaLocation = new Path("s3://target/");
    List<Path> sourceSubLocations = new ArrayList<>();

    TransferManagerFactory mockedTransferManagerFactory = Mockito.mock(TransferManagerFactory.class);
    TransferManager mockedTransferManager = Mockito.mock(TransferManager.class);
    when(mockedTransferManagerFactory.newInstance(any(AmazonS3.class), eq(s3S3CopierOptions)))
        .thenReturn(mockedTransferManager);
    Copy copy = Mockito.mock(Copy.class);
    when(copy.getProgress()).thenReturn(new TransferProgress());
    when(mockedTransferManager.copy(any(CopyObjectRequest.class), any(AmazonS3.class),
        any(TransferStateChangeListener.class))).thenReturn(copy);
    doThrow(new AmazonClientException("cause")).when(copy).waitForCompletion();
    S3S3Copier s3s3Copier = new S3S3Copier(sourceBaseLocation, sourceSubLocations, replicaLocation, s3ClientFactory,
        mockedTransferManagerFactory, listObjectsRequestFactory, registry, s3S3CopierOptions);
    try {
      s3s3Copier.copy();
      fail("exception should have been thrown");
    } catch (CircusTrainException e) {
      verify(mockedTransferManager).shutdownNow();
      assertThat(e.getCause().getMessage(), is("cause"));
    }
  }
  
  @Test
  public void copyDefaultCopierOptions() throws Exception {
    client.putObject("source", "data", inputData);
    Path sourceBaseLocation = new Path("s3://source/");
    Path replicaLocation = new Path("s3://target/");
    List<Path> sourceSubLocations = new ArrayList<>();

    TransferManagerFactory mockedTransferManagerFactory = Mockito.mock(TransferManagerFactory.class);
    TransferManager mockedTransferManager = Mockito.mock(TransferManager.class);
    when(mockedTransferManagerFactory.newInstance(any(AmazonS3.class), eq(s3S3CopierOptions)))
        .thenReturn(mockedTransferManager);
    Copy copy = Mockito.mock(Copy.class);
    when(mockedTransferManager.copy(any(CopyObjectRequest.class), any(AmazonS3.class),
        any(TransferStateChangeListener.class))).thenReturn(copy);
    TransferProgress transferProgress = new TransferProgress();
    when(copy.getProgress()).thenReturn(transferProgress);

    S3S3Copier s3s3Copier = new S3S3Copier(sourceBaseLocation, sourceSubLocations, replicaLocation, s3ClientFactory,
        mockedTransferManagerFactory, listObjectsRequestFactory, registry, s3S3CopierOptions);
    s3s3Copier.copy();
    ArgumentCaptor<CopyObjectRequest> argument = ArgumentCaptor.forClass(CopyObjectRequest.class);
    verify(mockedTransferManager).copy(argument.capture(), any(AmazonS3.class),any(TransferStateChangeListener.class));
    CopyObjectRequest copyObjectRequest = argument.getValue();
    assertNull(copyObjectRequest.getNewObjectMetadata());
  }

  @Test
  public void copyServerSideEncryption() throws Exception {
    client.putObject("source", "data", inputData);
    Path sourceBaseLocation = new Path("s3://source/");
    Path replicaLocation = new Path("s3://target/");
    List<Path> sourceSubLocations = new ArrayList<>();
    Map<String, Object> copierOptions = new HashMap<>();
    copierOptions.put(S3S3CopierOptions.Keys.S3_SERVER_SIDE_ENCRYPTION.keyName(), "true");
    S3S3CopierOptions customOptions = new S3S3CopierOptions(copierOptions);

    TransferManagerFactory mockedTransferManagerFactory = Mockito.mock(TransferManagerFactory.class);
    TransferManager mockedTransferManager = Mockito.mock(TransferManager.class);
    when(mockedTransferManagerFactory.newInstance(any(AmazonS3.class), eq(customOptions)))
        .thenReturn(mockedTransferManager);
    Copy copy = Mockito.mock(Copy.class);
    when(mockedTransferManager.copy(any(CopyObjectRequest.class), any(AmazonS3.class),
        any(TransferStateChangeListener.class))).thenReturn(copy);
    TransferProgress transferProgress = new TransferProgress();
    when(copy.getProgress()).thenReturn(transferProgress);

    S3S3Copier s3s3Copier = new S3S3Copier(sourceBaseLocation, sourceSubLocations, replicaLocation, s3ClientFactory,
        mockedTransferManagerFactory, listObjectsRequestFactory, registry, customOptions);
    s3s3Copier.copy();
    ArgumentCaptor<CopyObjectRequest> argument = ArgumentCaptor.forClass(CopyObjectRequest.class);
    verify(mockedTransferManager).copy(argument.capture(), any(AmazonS3.class),any(TransferStateChangeListener.class));
    CopyObjectRequest copyObjectRequest = argument.getValue();
    assertThat(copyObjectRequest.getNewObjectMetadata().getSSEAlgorithm(), is(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION));
  }

}
