/**
 * Copyright (C) 2016-2019 Expedia, Inc.
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
package com.hotels.bdp.circustrain.s3mapreducecpcopier;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

import static com.hotels.bdp.circustrain.s3mapreducecpcopier.S3MapReduceCpOptionsParser.CANNED_ACL;
import static com.hotels.bdp.circustrain.s3mapreducecpcopier.S3MapReduceCpOptionsParser.COPY_STRATEGY;
import static com.hotels.bdp.circustrain.s3mapreducecpcopier.S3MapReduceCpOptionsParser.CREDENTIAL_PROVIDER;
import static com.hotels.bdp.circustrain.s3mapreducecpcopier.S3MapReduceCpOptionsParser.IGNORE_FAILURES;
import static com.hotels.bdp.circustrain.s3mapreducecpcopier.S3MapReduceCpOptionsParser.LOG_PATH;
import static com.hotels.bdp.circustrain.s3mapreducecpcopier.S3MapReduceCpOptionsParser.MAX_MAPS;
import static com.hotels.bdp.circustrain.s3mapreducecpcopier.S3MapReduceCpOptionsParser.MULTIPART_UPLOAD_CHUNK_SIZE;
import static com.hotels.bdp.circustrain.s3mapreducecpcopier.S3MapReduceCpOptionsParser.MULTIPART_UPLOAD_THRESHOLD;
import static com.hotels.bdp.circustrain.s3mapreducecpcopier.S3MapReduceCpOptionsParser.NUMBER_OF_WORKERS_PER_MAP;
import static com.hotels.bdp.circustrain.s3mapreducecpcopier.S3MapReduceCpOptionsParser.REGION;
import static com.hotels.bdp.circustrain.s3mapreducecpcopier.S3MapReduceCpOptionsParser.S3_ENDPOINT_URI;
import static com.hotels.bdp.circustrain.s3mapreducecpcopier.S3MapReduceCpOptionsParser.S3_SERVER_SIDE_ENCRYPTION;
import static com.hotels.bdp.circustrain.s3mapreducecpcopier.S3MapReduceCpOptionsParser.STORAGE_CLASS;
import static com.hotels.bdp.circustrain.s3mapreducecpcopier.S3MapReduceCpOptionsParser.TASK_BANDWIDTH;
import static com.hotels.bdp.circustrain.s3mapreducecpcopier.S3MapReduceCpOptionsParser.UPLOAD_BUFFER_SIZE;
import static com.hotels.bdp.circustrain.s3mapreducecpcopier.S3MapReduceCpOptionsParser.UPLOAD_RETRY_COUNT;
import static com.hotels.bdp.circustrain.s3mapreducecpcopier.S3MapReduceCpOptionsParser.UPLOAD_RETRY_DELAY_MS;

import java.net.URI;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.model.StorageClass;

import com.hotels.bdp.circustrain.s3mapreducecp.ConfigurationVariable;
import com.hotels.bdp.circustrain.s3mapreducecp.S3MapReduceCpOptions;

public class S3MapReduceCpOptionsParserTest {

  private static final List<Path> SOURCES = Arrays.asList(new Path("hdfs://source"));
  private static final URI TARGET = URI.create("s3://target");
  private static final URI DEFAULT_CREDS_PROVIDER = URI.create("jceks://hdfs/foo/bar.jceks");

  private final Map<String, Object> copierOptions = new HashMap<>();
  private S3MapReduceCpOptionsParser parser;

  @Before
  public void init() {
    copierOptions.put(CREDENTIAL_PROVIDER, URI.create("localjceks://file/foo/bar.jceks"));
    copierOptions.put(MULTIPART_UPLOAD_CHUNK_SIZE, 4096);
    copierOptions.put(S3_SERVER_SIDE_ENCRYPTION, true);
    copierOptions.put(STORAGE_CLASS, StorageClass.Glacier.toString());
    copierOptions.put(TASK_BANDWIDTH, 1024);
    copierOptions.put(NUMBER_OF_WORKERS_PER_MAP, 12);
    copierOptions.put(MULTIPART_UPLOAD_THRESHOLD, 2048L);
    copierOptions.put(MAX_MAPS, 5);
    copierOptions.put(COPY_STRATEGY, "mycopystrategy");
    copierOptions.put(LOG_PATH, new Path("hdfs:///tmp/logs"));
    copierOptions.put(REGION, Regions.EU_WEST_1.getName());
    copierOptions.put(IGNORE_FAILURES, false);
    copierOptions.put(S3_ENDPOINT_URI, "http://s3.endpoint/");
    copierOptions.put(UPLOAD_RETRY_COUNT, 5);
    copierOptions.put(UPLOAD_RETRY_DELAY_MS, 520);
    copierOptions.put(UPLOAD_BUFFER_SIZE, 1024);
    copierOptions.put(CANNED_ACL, "bucket-owner-full-control");
    parser = new S3MapReduceCpOptionsParser(SOURCES, TARGET, DEFAULT_CREDS_PROVIDER);
  }

  private void assertDefaults(S3MapReduceCpOptions options) {
    assertThat(options.getCredentialsProvider(), is(URI.create("localjceks://file/foo/bar.jceks")));
    assertThat(options.getMultipartUploadPartSize(), is(4096L));
    assertThat(options.isS3ServerSideEncryption(), is(true));
    assertThat(options.getStorageClass(), is(StorageClass.Glacier.toString()));
    assertThat(options.getMaxBandwidth(), is(1024L));
    assertThat(options.getNumberOfUploadWorkers(), is(12));
    assertThat(options.getMultipartUploadThreshold(), is(2048L));
    assertThat(options.getMaxMaps(), is(5));
    assertThat(options.getCopyStrategy(), is("mycopystrategy"));
    assertThat(options.getLogPath(), is(new Path("hdfs:///tmp/logs")));
    assertThat(options.getRegion(), is(Regions.EU_WEST_1.getName()));
    assertThat(options.isIgnoreFailures(), is(false));
    assertThat(options.getS3EndpointUri(), is(URI.create("http://s3.endpoint/")));
    assertThat(options.getUploadRetryCount(), is(5));
    assertThat(options.getUploadRetryDelayMs(), is(520L));
    assertThat(options.getUploadBufferSize(), is(1024));
    assertThat(options.getCannedAcl(), is("bucket-owner-full-control"));
  }

  @Test
  public void typical() {
    S3MapReduceCpOptions options = parser.parse(copierOptions);
    assertThat(options, is(not(nullValue())));
    assertDefaults(options);
  }

  @Test
  public void missingCredentialsProviderWithNullDefault() {
    parser = new S3MapReduceCpOptionsParser(SOURCES, TARGET, null);
    copierOptions.remove(CREDENTIAL_PROVIDER);
    S3MapReduceCpOptions options = parser.parse(copierOptions);
    assertThat(options.getCredentialsProvider(), is(nullValue()));
  }

  @Test
  public void credentialsProviderWithNullDefault() {
    parser = new S3MapReduceCpOptionsParser(SOURCES, TARGET, null);
    S3MapReduceCpOptions options = parser.parse(copierOptions);
    assertThat(options.getCredentialsProvider(), is(URI.create("localjceks://file/foo/bar.jceks")));
  }

  @Test
  public void missingCredentialsProvider() {
    copierOptions.remove(CREDENTIAL_PROVIDER);
    S3MapReduceCpOptions options = parser.parse(copierOptions);
    assertThat(options.getCredentialsProvider(), is(DEFAULT_CREDS_PROVIDER));
  }

  @Test(expected = IllegalArgumentException.class)
  public void invalidCredentialsProvider() {
    copierOptions.put(CREDENTIAL_PROVIDER, "s3:");
    parser.parse(copierOptions);
  }

  @Test
  public void missingMultipartUploadPartSize() {
    copierOptions.remove(MULTIPART_UPLOAD_CHUNK_SIZE);
    S3MapReduceCpOptions options = parser.parse(copierOptions);
    assertThat(options.getMultipartUploadPartSize(),
        is(ConfigurationVariable.MULTIPART_UPLOAD_THRESHOLD.defaultLongValue()));
  }

  @Test(expected = IllegalArgumentException.class)
  public void zeroMultipartUploadPartSize() {
    copierOptions.put(MULTIPART_UPLOAD_CHUNK_SIZE, 0);
    parser.parse(copierOptions);
  }

  @Test(expected = IllegalArgumentException.class)
  public void negativeMultipartUploadPartSize() {
    copierOptions.put(MULTIPART_UPLOAD_CHUNK_SIZE, -1);
    parser.parse(copierOptions);
  }

  @Test
  public void noS3ServerSideEncryption() {
    copierOptions.put(S3_SERVER_SIDE_ENCRYPTION, false);
    S3MapReduceCpOptions options = parser.parse(copierOptions);
    assertThat(options.isS3ServerSideEncryption(), is(false));
  }

  @Test
  public void missingS3ServerSideEncryption() {
    copierOptions.remove(S3_SERVER_SIDE_ENCRYPTION);
    S3MapReduceCpOptions options = parser.parse(copierOptions);
    assertThat(options.isS3ServerSideEncryption(), is(true));
  }

  @Test
  public void missingStorageClass() {
    copierOptions.remove(STORAGE_CLASS);
    S3MapReduceCpOptions options = parser.parse(copierOptions);
    assertThat(options.getStorageClass(), is(ConfigurationVariable.STORAGE_CLASS.defaultValue()));
  }

  @Test(expected = IllegalArgumentException.class)
  public void invalidStorageClass() {
    copierOptions.put(STORAGE_CLASS, "storage");
    parser.parse(copierOptions);
  }

  @Test
  public void missingTaskBandwidth() {
    copierOptions.remove(TASK_BANDWIDTH);
    S3MapReduceCpOptions options = parser.parse(copierOptions);
    assertThat(options.getMaxBandwidth(), is(ConfigurationVariable.MAX_BANDWIDTH.defaultLongValue()));
  }

  @Test
  public void invalidTaskBandwidth() {
    copierOptions.put(TASK_BANDWIDTH, "number");
    S3MapReduceCpOptions options = parser.parse(copierOptions);
    assertThat(options.getMaxBandwidth(), is(ConfigurationVariable.MAX_BANDWIDTH.defaultLongValue()));
  }

  @Test(expected = IllegalArgumentException.class)
  public void zeroTaskBandwidth() {
    copierOptions.put(TASK_BANDWIDTH, 0);
    parser.parse(copierOptions);
  }

  @Test(expected = IllegalArgumentException.class)
  public void negativeTaskBandwidth() {
    copierOptions.put(TASK_BANDWIDTH, -1);
    parser.parse(copierOptions);
  }

  @Test
  public void missingNumberOfUploadWorkers() {
    copierOptions.remove(NUMBER_OF_WORKERS_PER_MAP);
    S3MapReduceCpOptions options = parser.parse(copierOptions);
    assertThat(options.getNumberOfUploadWorkers(),
        is(ConfigurationVariable.NUMBER_OF_UPLOAD_WORKERS.defaultIntValue()));
  }

  @Test
  public void invalidNumberOfUploadWorkers() {
    copierOptions.put(NUMBER_OF_WORKERS_PER_MAP, "number");
    S3MapReduceCpOptions options = parser.parse(copierOptions);
    assertThat(options.getNumberOfUploadWorkers(),
        is(ConfigurationVariable.NUMBER_OF_UPLOAD_WORKERS.defaultIntValue()));
  }

  @Test(expected = IllegalArgumentException.class)
  public void zeroNumberOfUploadWorkers() {
    copierOptions.put(NUMBER_OF_WORKERS_PER_MAP, 0);
    parser.parse(copierOptions);
  }

  @Test(expected = IllegalArgumentException.class)
  public void negativeNumberOfUploadWorkers() {
    copierOptions.put(NUMBER_OF_WORKERS_PER_MAP, -1);
    parser.parse(copierOptions);
  }

  @Test
  public void missingMultipartUploadThreshold() {
    copierOptions.remove(MULTIPART_UPLOAD_THRESHOLD);
    S3MapReduceCpOptions options = parser.parse(copierOptions);
    assertThat(options.getMultipartUploadThreshold(),
        is(ConfigurationVariable.MULTIPART_UPLOAD_THRESHOLD.defaultLongValue()));
  }

  @Test
  public void invalidMultipartUploadThreshold() {
    copierOptions.put(MULTIPART_UPLOAD_THRESHOLD, "number");
    S3MapReduceCpOptions options = parser.parse(copierOptions);
    assertThat(options.getMultipartUploadThreshold(),
        is(ConfigurationVariable.MULTIPART_UPLOAD_THRESHOLD.defaultLongValue()));
  }

  @Test(expected = IllegalArgumentException.class)
  public void zeroMultipartUploadThreshold() {
    copierOptions.put(MULTIPART_UPLOAD_THRESHOLD, 0);
    parser.parse(copierOptions);
  }

  @Test(expected = IllegalArgumentException.class)
  public void negativeMultipartUploadThreshold() {
    copierOptions.put(MULTIPART_UPLOAD_THRESHOLD, -1);
    parser.parse(copierOptions);
  }

  @Test
  public void missingMaxMaps() {
    copierOptions.remove(MAX_MAPS);
    S3MapReduceCpOptions options = parser.parse(copierOptions);
    assertThat(options.getMaxMaps(), is(ConfigurationVariable.MAX_MAPS.defaultIntValue()));
  }

  @Test
  public void invalidMaxMaps() {
    copierOptions.put(MAX_MAPS, "number");
    S3MapReduceCpOptions options = parser.parse(copierOptions);
    assertThat(options.getMaxMaps(), is(ConfigurationVariable.MAX_MAPS.defaultIntValue()));
  }

  @Test(expected = IllegalArgumentException.class)
  public void zeroMaxMaps() {
    copierOptions.put(MAX_MAPS, 0);
    parser.parse(copierOptions);
  }

  @Test(expected = IllegalArgumentException.class)
  public void negativeMaxMaps() {
    copierOptions.put(MAX_MAPS, -1);
    parser.parse(copierOptions);
  }

  @Test
  public void missingCopyStrategy() {
    copierOptions.remove(COPY_STRATEGY);
    S3MapReduceCpOptions options = parser.parse(copierOptions);
    assertThat(options.getCopyStrategy(), is(ConfigurationVariable.COPY_STRATEGY.defaultValue()));
  }

  @Test
  public void missingLogPath() {
    copierOptions.remove(LOG_PATH);
    S3MapReduceCpOptions options = parser.parse(copierOptions);
    assertThat(options.getLogPath(), is(nullValue()));
  }

  @Test
  public void missingRegion() {
    copierOptions.remove(REGION);
    S3MapReduceCpOptions options = parser.parse(copierOptions);
    assertThat(options.getRegion(), is(ConfigurationVariable.REGION.defaultValue()));
  }

  @Test(expected = IllegalArgumentException.class)
  public void invalidRegion() {
    copierOptions.put(REGION, "my-region");
    parser.parse(copierOptions);
  }

  @Test
  public void missingIgnoreFailures() {
    copierOptions.remove(IGNORE_FAILURES);
    S3MapReduceCpOptions options = parser.parse(copierOptions);
    assertThat(options.isIgnoreFailures(), is(ConfigurationVariable.IGNORE_FAILURES.defaultBooleanValue()));
  }

  @Test
  public void missingS3EnpointUri() {
    copierOptions.remove(S3_ENDPOINT_URI);
    S3MapReduceCpOptions options = parser.parse(copierOptions);
    assertThat(options.getS3EndpointUri(), is(ConfigurationVariable.S3_ENDPOINT_URI.defaultURIValue()));
  }

  @Test(expected = IllegalArgumentException.class)
  public void invalidS3EndpointUri() {
    copierOptions.put(S3_ENDPOINT_URI, "s3:");
    parser.parse(copierOptions);
  }

  public void missingUploadRetryCount() {
    copierOptions.remove(UPLOAD_RETRY_COUNT);
    S3MapReduceCpOptions options = parser.parse(copierOptions);
    assertThat(options.getUploadRetryCount(), is(ConfigurationVariable.UPLOAD_RETRY_COUNT.defaultIntValue()));
  }

  @Test
  public void invalidUploadRetryCount() {
    copierOptions.put(UPLOAD_RETRY_COUNT, "number");
    S3MapReduceCpOptions options = parser.parse(copierOptions);
    assertThat(options.getUploadRetryCount(), is(ConfigurationVariable.UPLOAD_RETRY_COUNT.defaultIntValue()));
  }

  @Test
  public void zeroUploadRetryCount() {
    copierOptions.put(UPLOAD_RETRY_COUNT, 0);
    S3MapReduceCpOptions options = parser.parse(copierOptions);
    assertThat(options.getUploadRetryCount(), is(0));
  }

  @Test(expected = IllegalArgumentException.class)
  public void negativeUploadRetryCount() {
    copierOptions.put(UPLOAD_RETRY_COUNT, -1);
    parser.parse(copierOptions);
  }

  @Test
  public void missingUploadRetryDelaysMs() {
    copierOptions.remove(UPLOAD_RETRY_DELAY_MS);
    S3MapReduceCpOptions options = parser.parse(copierOptions);
    assertThat(options.getUploadRetryDelayMs(), is(ConfigurationVariable.UPLOAD_RETRY_DELAY_MS.defaultLongValue()));
  }

  @Test
  public void invalidUploadRetryDelaysMs() {
    copierOptions.put(UPLOAD_RETRY_DELAY_MS, "number");
    S3MapReduceCpOptions options = parser.parse(copierOptions);
    assertThat(options.getUploadRetryDelayMs(), is(ConfigurationVariable.UPLOAD_RETRY_DELAY_MS.defaultLongValue()));
  }

  @Test
  public void zeroUploadRetryDelaysMs() {
    copierOptions.put(UPLOAD_RETRY_DELAY_MS, 0);
    S3MapReduceCpOptions options = parser.parse(copierOptions);
    assertThat(options.getUploadRetryDelayMs(), is(0L));
  }

  @Test(expected = IllegalArgumentException.class)
  public void negativeUploadRetryDelaysMs() {
    copierOptions.put(UPLOAD_RETRY_DELAY_MS, -1);
    parser.parse(copierOptions);
  }

  @Test
  public void missingUploadBufferSize() {
    copierOptions.remove(UPLOAD_BUFFER_SIZE);
    S3MapReduceCpOptions options = parser.parse(copierOptions);
    assertThat(options.getUploadBufferSize(), is(ConfigurationVariable.UPLOAD_BUFFER_SIZE.defaultIntValue()));
  }

  @Test
  public void invalidUploadBufferSize() {
    copierOptions.put(UPLOAD_BUFFER_SIZE, "number");
    S3MapReduceCpOptions options = parser.parse(copierOptions);
    assertThat(options.getUploadBufferSize(), is(ConfigurationVariable.UPLOAD_BUFFER_SIZE.defaultIntValue()));
  }

  @Test
  public void zeroUploadBufferSize() {
    copierOptions.put(UPLOAD_BUFFER_SIZE, 0);
    S3MapReduceCpOptions options = parser.parse(copierOptions);
    assertThat(options.getUploadBufferSize(), is(0));
  }

  @Test(expected = IllegalArgumentException.class)
  public void negativeUploadBufferSize() {
    copierOptions.put(UPLOAD_BUFFER_SIZE, -1);
    parser.parse(copierOptions);
  }

  @Test
  public void missingCannedAcl() {
    copierOptions.remove(CANNED_ACL);
    S3MapReduceCpOptions options = parser.parse(copierOptions);
    assertThat(options.getCannedAcl(), is(ConfigurationVariable.CANNED_ACL.defaultValue()));
  }

  @Test(expected = IllegalArgumentException.class)
  public void invalidCannedAcl() {
    copierOptions.put(CANNED_ACL, "my-canned-acl");
    parser.parse(copierOptions);
  }
}
