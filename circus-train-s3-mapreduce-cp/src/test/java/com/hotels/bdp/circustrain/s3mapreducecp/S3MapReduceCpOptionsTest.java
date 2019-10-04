/**
 * Copyright (C) 2016-2019 Expedia Inc.
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
package com.hotels.bdp.circustrain.s3mapreducecp;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

import java.net.URI;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.junit.Test;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.StorageClass;

public class S3MapReduceCpOptionsTest {

  private static final List<Path> SOURCES = Arrays.asList(new Path("source1"), new Path("source2"));
  private static final URI TARGET = URI.create("target");

  @Test
  public void defaultValues() {
    S3MapReduceCpOptions options = new S3MapReduceCpOptions();
    assertThat(options.isHelp(), is(false));
    assertThat(options.isBlocking(), is(true));
    assertThat(options.getSources(), is(nullValue()));
    assertThat(options.getTarget(), is(nullValue()));
    assertThat(options.getCredentialsProvider(), is(nullValue()));
    assertThat(options.getMultipartUploadPartSize(), is(5L * 1024 * 1024));
    assertThat(options.isS3ServerSideEncryption(), is(false));
    assertThat(options.getStorageClass(), is(StorageClass.Standard.toString()));
    assertThat(options.getMaxBandwidth(), is(100L));
    assertThat(options.getNumberOfUploadWorkers(), is(20));
    assertThat(options.getMultipartUploadThreshold(), is(16L * 1024 * 1024));
    assertThat(options.getMaxMaps(), is(20));
    assertThat(options.getCopyStrategy(), is("uniformsize"));
    assertThat(options.getLogPath(), is(nullValue()));
    assertThat(options.getRegion(), is(nullValue()));
    assertThat(options.isIgnoreFailures(), is(false));
    assertThat(options.getS3EndpointUri(), is(nullValue()));
    assertThat(options.getUploadRetryCount(), is(3));
    assertThat(options.getUploadRetryDelayMs(), is(300L));
    assertThat(options.getUploadBufferSize(), is(0));
    assertThat(options.getCannedAcl(), is(nullValue()));
    assertThat(options.getAssumeRole(), is(nullValue()));
  }

  @Test
  public void builderTypical() {
    S3MapReduceCpOptions options = S3MapReduceCpOptions.builder(SOURCES, TARGET).build();
    assertThat(options.isHelp(), is(false));
    assertThat(options.isBlocking(), is(true));
    assertThat(options.getSources(), is(SOURCES));
    assertThat(options.getTarget(), is(TARGET));
    assertThat(options.getCredentialsProvider(), is(ConfigurationVariable.CREDENTIAL_PROVIDER.defaultURIValue()));
    assertThat(options.getMultipartUploadPartSize(),
        is(ConfigurationVariable.MINIMUM_UPLOAD_PART_SIZE.defaultLongValue()));
    assertThat(options.isS3ServerSideEncryption(),
        is(ConfigurationVariable.S3_SERVER_SIDE_ENCRYPTION.defaultBooleanValue()));
    assertThat(options.getStorageClass(), is(ConfigurationVariable.STORAGE_CLASS.defaultValue()));
    assertThat(options.getMaxBandwidth(), is(ConfigurationVariable.MAX_BANDWIDTH.defaultLongValue()));
    assertThat(options.getNumberOfUploadWorkers(),
        is(ConfigurationVariable.NUMBER_OF_UPLOAD_WORKERS.defaultIntValue()));
    assertThat(options.getMultipartUploadThreshold(),
        is(ConfigurationVariable.MULTIPART_UPLOAD_THRESHOLD.defaultLongValue()));
    assertThat(options.getMaxMaps(), is(ConfigurationVariable.MAX_MAPS.defaultIntValue()));
    assertThat(options.getCopyStrategy(), is(ConfigurationVariable.COPY_STRATEGY.defaultValue()));
    assertThat(options.getLogPath(), is(nullValue()));
    assertThat(options.getRegion(), is(ConfigurationVariable.REGION.defaultValue()));
    assertThat(options.isIgnoreFailures(), is(ConfigurationVariable.IGNORE_FAILURES.defaultBooleanValue()));
    assertThat(options.getS3EndpointUri(), is(ConfigurationVariable.S3_ENDPOINT_URI.defaultURIValue()));
    assertThat(options.getUploadRetryCount(), is(ConfigurationVariable.UPLOAD_RETRY_COUNT.defaultIntValue()));
    assertThat(options.getUploadRetryDelayMs(), is(ConfigurationVariable.UPLOAD_RETRY_DELAY_MS.defaultLongValue()));
    assertThat(options.getUploadBufferSize(), is(ConfigurationVariable.UPLOAD_BUFFER_SIZE.defaultIntValue()));
    assertThat(options.getCannedAcl(), is(ConfigurationVariable.CANNED_ACL.defaultValue()));
    assertThat(options.getAssumeRole(), is(ConfigurationVariable.ASSUME_ROLE.defaultValue()));
  }

  @Test
  public void builderWithCredentialsProvider() {
    S3MapReduceCpOptions options = S3MapReduceCpOptions
        .builder(SOURCES, TARGET)
        .credentialsProvider(URI.create("creds"))
        .build();
    assertThat(options.isHelp(), is(false));
    assertThat(options.isBlocking(), is(true));
    assertThat(options.getSources(), is(SOURCES));
    assertThat(options.getTarget(), is(TARGET));
    assertThat(options.getCredentialsProvider(), is(URI.create("creds")));
    assertThat(options.getMultipartUploadPartSize(),
        is(ConfigurationVariable.MINIMUM_UPLOAD_PART_SIZE.defaultLongValue()));
    assertThat(options.isS3ServerSideEncryption(),
        is(ConfigurationVariable.S3_SERVER_SIDE_ENCRYPTION.defaultBooleanValue()));
    assertThat(options.getStorageClass(), is(ConfigurationVariable.STORAGE_CLASS.defaultValue()));
    assertThat(options.getMaxBandwidth(), is(ConfigurationVariable.MAX_BANDWIDTH.defaultLongValue()));
    assertThat(options.getNumberOfUploadWorkers(),
        is(ConfigurationVariable.NUMBER_OF_UPLOAD_WORKERS.defaultIntValue()));
    assertThat(options.getMultipartUploadThreshold(),
        is(ConfigurationVariable.MULTIPART_UPLOAD_THRESHOLD.defaultLongValue()));
    assertThat(options.getMaxMaps(), is(ConfigurationVariable.MAX_MAPS.defaultIntValue()));
    assertThat(options.getCopyStrategy(), is(ConfigurationVariable.COPY_STRATEGY.defaultValue()));
    assertThat(options.getLogPath(), is(nullValue()));
    assertThat(options.getRegion(), is(ConfigurationVariable.REGION.defaultValue()));
    assertThat(options.isIgnoreFailures(), is(ConfigurationVariable.IGNORE_FAILURES.defaultBooleanValue()));
    assertThat(options.getS3EndpointUri(), is(ConfigurationVariable.S3_ENDPOINT_URI.defaultURIValue()));
    assertThat(options.getUploadRetryCount(), is(ConfigurationVariable.UPLOAD_RETRY_COUNT.defaultIntValue()));
    assertThat(options.getUploadRetryDelayMs(), is(ConfigurationVariable.UPLOAD_RETRY_DELAY_MS.defaultLongValue()));
    assertThat(options.getUploadBufferSize(), is(ConfigurationVariable.UPLOAD_BUFFER_SIZE.defaultIntValue()));
    assertThat(options.getCannedAcl(), is(ConfigurationVariable.CANNED_ACL.defaultValue()));
    assertThat(options.getAssumeRole(), is(ConfigurationVariable.ASSUME_ROLE.defaultValue()));
  }

  @Test
  public void builderWithMultipartUploadPartSize() {
    S3MapReduceCpOptions options = S3MapReduceCpOptions.builder(SOURCES, TARGET).multipartUploadPartSize(89L).build();
    assertThat(options.isHelp(), is(false));
    assertThat(options.isBlocking(), is(true));
    assertThat(options.getSources(), is(SOURCES));
    assertThat(options.getTarget(), is(TARGET));
    assertThat(options.getCredentialsProvider(), is(ConfigurationVariable.CREDENTIAL_PROVIDER.defaultURIValue()));
    assertThat(options.getMultipartUploadPartSize(), is(89L));
    assertThat(options.isS3ServerSideEncryption(),
        is(ConfigurationVariable.S3_SERVER_SIDE_ENCRYPTION.defaultBooleanValue()));
    assertThat(options.getStorageClass(), is(ConfigurationVariable.STORAGE_CLASS.defaultValue()));
    assertThat(options.getMaxBandwidth(), is(ConfigurationVariable.MAX_BANDWIDTH.defaultLongValue()));
    assertThat(options.getNumberOfUploadWorkers(),
        is(ConfigurationVariable.NUMBER_OF_UPLOAD_WORKERS.defaultIntValue()));
    assertThat(options.getMultipartUploadThreshold(),
        is(ConfigurationVariable.MULTIPART_UPLOAD_THRESHOLD.defaultLongValue()));
    assertThat(options.getMaxMaps(), is(ConfigurationVariable.MAX_MAPS.defaultIntValue()));
    assertThat(options.getCopyStrategy(), is(ConfigurationVariable.COPY_STRATEGY.defaultValue()));
    assertThat(options.getLogPath(), is(nullValue()));
    assertThat(options.getRegion(), is(ConfigurationVariable.REGION.defaultValue()));
    assertThat(options.isIgnoreFailures(), is(ConfigurationVariable.IGNORE_FAILURES.defaultBooleanValue()));
    assertThat(options.getS3EndpointUri(), is(ConfigurationVariable.S3_ENDPOINT_URI.defaultURIValue()));
    assertThat(options.getUploadRetryCount(), is(ConfigurationVariable.UPLOAD_RETRY_COUNT.defaultIntValue()));
    assertThat(options.getUploadRetryDelayMs(), is(ConfigurationVariable.UPLOAD_RETRY_DELAY_MS.defaultLongValue()));
    assertThat(options.getUploadBufferSize(), is(ConfigurationVariable.UPLOAD_BUFFER_SIZE.defaultIntValue()));
    assertThat(options.getCannedAcl(), is(ConfigurationVariable.CANNED_ACL.defaultValue()));
    assertThat(options.getAssumeRole(), is(ConfigurationVariable.ASSUME_ROLE.defaultValue()));
  }

  @Test
  public void builderWithS3ServerSideEncryption() {
    S3MapReduceCpOptions options = S3MapReduceCpOptions.builder(SOURCES, TARGET).s3ServerSideEncryption(true).build();
    assertThat(options.isHelp(), is(false));
    assertThat(options.isBlocking(), is(true));
    assertThat(options.getSources(), is(SOURCES));
    assertThat(options.getTarget(), is(TARGET));
    assertThat(options.getCredentialsProvider(), is(ConfigurationVariable.CREDENTIAL_PROVIDER.defaultURIValue()));
    assertThat(options.getMultipartUploadPartSize(),
        is(ConfigurationVariable.MINIMUM_UPLOAD_PART_SIZE.defaultLongValue()));
    assertThat(options.isS3ServerSideEncryption(), is(true));
    assertThat(options.getStorageClass(), is(ConfigurationVariable.STORAGE_CLASS.defaultValue()));
    assertThat(options.getMaxBandwidth(), is(ConfigurationVariable.MAX_BANDWIDTH.defaultLongValue()));
    assertThat(options.getNumberOfUploadWorkers(),
        is(ConfigurationVariable.NUMBER_OF_UPLOAD_WORKERS.defaultIntValue()));
    assertThat(options.getMultipartUploadThreshold(),
        is(ConfigurationVariable.MULTIPART_UPLOAD_THRESHOLD.defaultLongValue()));
    assertThat(options.getMaxMaps(), is(ConfigurationVariable.MAX_MAPS.defaultIntValue()));
    assertThat(options.getCopyStrategy(), is(ConfigurationVariable.COPY_STRATEGY.defaultValue()));
    assertThat(options.getLogPath(), is(nullValue()));
    assertThat(options.getRegion(), is(ConfigurationVariable.REGION.defaultValue()));
    assertThat(options.isIgnoreFailures(), is(ConfigurationVariable.IGNORE_FAILURES.defaultBooleanValue()));
    assertThat(options.getS3EndpointUri(), is(ConfigurationVariable.S3_ENDPOINT_URI.defaultURIValue()));
    assertThat(options.getUploadRetryCount(), is(ConfigurationVariable.UPLOAD_RETRY_COUNT.defaultIntValue()));
    assertThat(options.getUploadRetryDelayMs(), is(ConfigurationVariable.UPLOAD_RETRY_DELAY_MS.defaultLongValue()));
    assertThat(options.getUploadBufferSize(), is(ConfigurationVariable.UPLOAD_BUFFER_SIZE.defaultIntValue()));
    assertThat(options.getCannedAcl(), is(ConfigurationVariable.CANNED_ACL.defaultValue()));
    assertThat(options.getAssumeRole(), is(ConfigurationVariable.ASSUME_ROLE.defaultValue()));
  }

  @Test
  public void builderWithStorageClass() {
    S3MapReduceCpOptions options = S3MapReduceCpOptions
        .builder(SOURCES, TARGET)
        .storageClass(StorageClass.Glacier.toString())
        .build();
    assertThat(options.isHelp(), is(false));
    assertThat(options.isBlocking(), is(true));
    assertThat(options.getSources(), is(SOURCES));
    assertThat(options.getTarget(), is(TARGET));
    assertThat(options.getCredentialsProvider(), is(ConfigurationVariable.CREDENTIAL_PROVIDER.defaultURIValue()));
    assertThat(options.getMultipartUploadPartSize(),
        is(ConfigurationVariable.MINIMUM_UPLOAD_PART_SIZE.defaultLongValue()));
    assertThat(options.isS3ServerSideEncryption(),
        is(ConfigurationVariable.S3_SERVER_SIDE_ENCRYPTION.defaultBooleanValue()));
    assertThat(options.getStorageClass(), is(StorageClass.Glacier.toString()));
    assertThat(options.getMaxBandwidth(), is(ConfigurationVariable.MAX_BANDWIDTH.defaultLongValue()));
    assertThat(options.getNumberOfUploadWorkers(),
        is(ConfigurationVariable.NUMBER_OF_UPLOAD_WORKERS.defaultIntValue()));
    assertThat(options.getMultipartUploadThreshold(),
        is(ConfigurationVariable.MULTIPART_UPLOAD_THRESHOLD.defaultLongValue()));
    assertThat(options.getMaxMaps(), is(ConfigurationVariable.MAX_MAPS.defaultIntValue()));
    assertThat(options.getCopyStrategy(), is(ConfigurationVariable.COPY_STRATEGY.defaultValue()));
    assertThat(options.getLogPath(), is(nullValue()));
    assertThat(options.getRegion(), is(ConfigurationVariable.REGION.defaultValue()));
    assertThat(options.isIgnoreFailures(), is(ConfigurationVariable.IGNORE_FAILURES.defaultBooleanValue()));
    assertThat(options.getS3EndpointUri(), is(ConfigurationVariable.S3_ENDPOINT_URI.defaultURIValue()));
    assertThat(options.getUploadRetryCount(), is(ConfigurationVariable.UPLOAD_RETRY_COUNT.defaultIntValue()));
    assertThat(options.getUploadRetryDelayMs(), is(ConfigurationVariable.UPLOAD_RETRY_DELAY_MS.defaultLongValue()));
    assertThat(options.getUploadBufferSize(), is(ConfigurationVariable.UPLOAD_BUFFER_SIZE.defaultIntValue()));
    assertThat(options.getCannedAcl(), is(ConfigurationVariable.CANNED_ACL.defaultValue()));
    assertThat(options.getAssumeRole(), is(ConfigurationVariable.ASSUME_ROLE.defaultValue()));
  }

  @Test
  public void builderWithMaxBandwith() {
    S3MapReduceCpOptions options = S3MapReduceCpOptions.builder(SOURCES, TARGET).maxBandwidth(24L).build();
    assertThat(options.isHelp(), is(false));
    assertThat(options.isBlocking(), is(true));
    assertThat(options.getSources(), is(SOURCES));
    assertThat(options.getTarget(), is(TARGET));
    assertThat(options.getCredentialsProvider(), is(ConfigurationVariable.CREDENTIAL_PROVIDER.defaultURIValue()));
    assertThat(options.getMultipartUploadPartSize(),
        is(ConfigurationVariable.MINIMUM_UPLOAD_PART_SIZE.defaultLongValue()));
    assertThat(options.isS3ServerSideEncryption(),
        is(ConfigurationVariable.S3_SERVER_SIDE_ENCRYPTION.defaultBooleanValue()));
    assertThat(options.getStorageClass(), is(ConfigurationVariable.STORAGE_CLASS.defaultValue()));
    assertThat(options.getMaxBandwidth(), is(24L));
    assertThat(options.getNumberOfUploadWorkers(),
        is(ConfigurationVariable.NUMBER_OF_UPLOAD_WORKERS.defaultIntValue()));
    assertThat(options.getMultipartUploadThreshold(),
        is(ConfigurationVariable.MULTIPART_UPLOAD_THRESHOLD.defaultLongValue()));
    assertThat(options.getMaxMaps(), is(ConfigurationVariable.MAX_MAPS.defaultIntValue()));
    assertThat(options.getCopyStrategy(), is(ConfigurationVariable.COPY_STRATEGY.defaultValue()));
    assertThat(options.getLogPath(), is(nullValue()));
    assertThat(options.getRegion(), is(ConfigurationVariable.REGION.defaultValue()));
    assertThat(options.isIgnoreFailures(), is(ConfigurationVariable.IGNORE_FAILURES.defaultBooleanValue()));
    assertThat(options.getS3EndpointUri(), is(ConfigurationVariable.S3_ENDPOINT_URI.defaultURIValue()));
    assertThat(options.getUploadRetryCount(), is(ConfigurationVariable.UPLOAD_RETRY_COUNT.defaultIntValue()));
    assertThat(options.getUploadRetryDelayMs(), is(ConfigurationVariable.UPLOAD_RETRY_DELAY_MS.defaultLongValue()));
    assertThat(options.getUploadBufferSize(), is(ConfigurationVariable.UPLOAD_BUFFER_SIZE.defaultIntValue()));
    assertThat(options.getCannedAcl(), is(ConfigurationVariable.CANNED_ACL.defaultValue()));
    assertThat(options.getAssumeRole(), is(ConfigurationVariable.ASSUME_ROLE.defaultValue()));
  }

  @Test
  public void builderWithNumberOfUploadWorkers() {
    S3MapReduceCpOptions options = S3MapReduceCpOptions.builder(SOURCES, TARGET).numberOfUploadWorkers(45).build();
    assertThat(options.isHelp(), is(false));
    assertThat(options.isBlocking(), is(true));
    assertThat(options.getSources(), is(SOURCES));
    assertThat(options.getTarget(), is(TARGET));
    assertThat(options.getCredentialsProvider(), is(ConfigurationVariable.CREDENTIAL_PROVIDER.defaultURIValue()));
    assertThat(options.getMultipartUploadPartSize(),
        is(ConfigurationVariable.MINIMUM_UPLOAD_PART_SIZE.defaultLongValue()));
    assertThat(options.isS3ServerSideEncryption(),
        is(ConfigurationVariable.S3_SERVER_SIDE_ENCRYPTION.defaultBooleanValue()));
    assertThat(options.getStorageClass(), is(ConfigurationVariable.STORAGE_CLASS.defaultValue()));
    assertThat(options.getMaxBandwidth(), is(ConfigurationVariable.MAX_BANDWIDTH.defaultLongValue()));
    assertThat(options.getNumberOfUploadWorkers(), is(45));
    assertThat(options.getMultipartUploadThreshold(),
        is(ConfigurationVariable.MULTIPART_UPLOAD_THRESHOLD.defaultLongValue()));
    assertThat(options.getMaxMaps(), is(ConfigurationVariable.MAX_MAPS.defaultIntValue()));
    assertThat(options.getCopyStrategy(), is(ConfigurationVariable.COPY_STRATEGY.defaultValue()));
    assertThat(options.getLogPath(), is(nullValue()));
    assertThat(options.getRegion(), is(ConfigurationVariable.REGION.defaultValue()));
    assertThat(options.isIgnoreFailures(), is(ConfigurationVariable.IGNORE_FAILURES.defaultBooleanValue()));
    assertThat(options.getS3EndpointUri(), is(ConfigurationVariable.S3_ENDPOINT_URI.defaultURIValue()));
    assertThat(options.getUploadRetryCount(), is(ConfigurationVariable.UPLOAD_RETRY_COUNT.defaultIntValue()));
    assertThat(options.getUploadRetryDelayMs(), is(ConfigurationVariable.UPLOAD_RETRY_DELAY_MS.defaultLongValue()));
    assertThat(options.getUploadBufferSize(), is(ConfigurationVariable.UPLOAD_BUFFER_SIZE.defaultIntValue()));
    assertThat(options.getCannedAcl(), is(ConfigurationVariable.CANNED_ACL.defaultValue()));
    assertThat(options.getAssumeRole(), is(ConfigurationVariable.ASSUME_ROLE.defaultValue()));
  }

  @Test
  public void builderWithMultipartUploadThreshold() {
    S3MapReduceCpOptions options = S3MapReduceCpOptions.builder(SOURCES, TARGET).multipartUploadThreshold(48L).build();
    assertThat(options.isHelp(), is(false));
    assertThat(options.isBlocking(), is(true));
    assertThat(options.getSources(), is(SOURCES));
    assertThat(options.getTarget(), is(TARGET));
    assertThat(options.getCredentialsProvider(), is(ConfigurationVariable.CREDENTIAL_PROVIDER.defaultURIValue()));
    assertThat(options.getMultipartUploadPartSize(),
        is(ConfigurationVariable.MINIMUM_UPLOAD_PART_SIZE.defaultLongValue()));
    assertThat(options.isS3ServerSideEncryption(),
        is(ConfigurationVariable.S3_SERVER_SIDE_ENCRYPTION.defaultBooleanValue()));
    assertThat(options.getStorageClass(), is(ConfigurationVariable.STORAGE_CLASS.defaultValue()));
    assertThat(options.getMaxBandwidth(), is(ConfigurationVariable.MAX_BANDWIDTH.defaultLongValue()));
    assertThat(options.getNumberOfUploadWorkers(),
        is(ConfigurationVariable.NUMBER_OF_UPLOAD_WORKERS.defaultIntValue()));
    assertThat(options.getMultipartUploadThreshold(), is(48L));
    assertThat(options.getMaxMaps(), is(ConfigurationVariable.MAX_MAPS.defaultIntValue()));
    assertThat(options.getCopyStrategy(), is(ConfigurationVariable.COPY_STRATEGY.defaultValue()));
    assertThat(options.getLogPath(), is(nullValue()));
    assertThat(options.getRegion(), is(ConfigurationVariable.REGION.defaultValue()));
    assertThat(options.isIgnoreFailures(), is(ConfigurationVariable.IGNORE_FAILURES.defaultBooleanValue()));
    assertThat(options.getS3EndpointUri(), is(ConfigurationVariable.S3_ENDPOINT_URI.defaultURIValue()));
    assertThat(options.getUploadRetryCount(), is(ConfigurationVariable.UPLOAD_RETRY_COUNT.defaultIntValue()));
    assertThat(options.getUploadRetryDelayMs(), is(ConfigurationVariable.UPLOAD_RETRY_DELAY_MS.defaultLongValue()));
    assertThat(options.getUploadBufferSize(), is(ConfigurationVariable.UPLOAD_BUFFER_SIZE.defaultIntValue()));
    assertThat(options.getCannedAcl(), is(ConfigurationVariable.CANNED_ACL.defaultValue()));
    assertThat(options.getAssumeRole(), is(ConfigurationVariable.ASSUME_ROLE.defaultValue()));
  }

  @Test
  public void builderWithMaxMaps() {
    S3MapReduceCpOptions options = S3MapReduceCpOptions.builder(SOURCES, TARGET).maxMaps(67).build();
    assertThat(options.isHelp(), is(false));
    assertThat(options.isBlocking(), is(true));
    assertThat(options.getSources(), is(SOURCES));
    assertThat(options.getTarget(), is(TARGET));
    assertThat(options.getCredentialsProvider(), is(ConfigurationVariable.CREDENTIAL_PROVIDER.defaultURIValue()));
    assertThat(options.getMultipartUploadPartSize(),
        is(ConfigurationVariable.MINIMUM_UPLOAD_PART_SIZE.defaultLongValue()));
    assertThat(options.isS3ServerSideEncryption(),
        is(ConfigurationVariable.S3_SERVER_SIDE_ENCRYPTION.defaultBooleanValue()));
    assertThat(options.getStorageClass(), is(ConfigurationVariable.STORAGE_CLASS.defaultValue()));
    assertThat(options.getMaxBandwidth(), is(ConfigurationVariable.MAX_BANDWIDTH.defaultLongValue()));
    assertThat(options.getNumberOfUploadWorkers(),
        is(ConfigurationVariable.NUMBER_OF_UPLOAD_WORKERS.defaultIntValue()));
    assertThat(options.getMultipartUploadThreshold(),
        is(ConfigurationVariable.MULTIPART_UPLOAD_THRESHOLD.defaultLongValue()));
    assertThat(options.getMaxMaps(), is(67));
    assertThat(options.getCopyStrategy(), is(ConfigurationVariable.COPY_STRATEGY.defaultValue()));
    assertThat(options.getLogPath(), is(nullValue()));
    assertThat(options.getRegion(), is(ConfigurationVariable.REGION.defaultValue()));
    assertThat(options.isIgnoreFailures(), is(ConfigurationVariable.IGNORE_FAILURES.defaultBooleanValue()));
    assertThat(options.getS3EndpointUri(), is(ConfigurationVariable.S3_ENDPOINT_URI.defaultURIValue()));
    assertThat(options.getUploadRetryCount(), is(ConfigurationVariable.UPLOAD_RETRY_COUNT.defaultIntValue()));
    assertThat(options.getUploadRetryDelayMs(), is(ConfigurationVariable.UPLOAD_RETRY_DELAY_MS.defaultLongValue()));
    assertThat(options.getUploadBufferSize(), is(ConfigurationVariable.UPLOAD_BUFFER_SIZE.defaultIntValue()));
    assertThat(options.getCannedAcl(), is(ConfigurationVariable.CANNED_ACL.defaultValue()));
    assertThat(options.getAssumeRole(), is(ConfigurationVariable.ASSUME_ROLE.defaultValue()));
  }

  @Test
  public void builderWithCopyStrategy() {
    S3MapReduceCpOptions options = S3MapReduceCpOptions.builder(SOURCES, TARGET).copyStrategy("mystrategy").build();
    assertThat(options.isHelp(), is(false));
    assertThat(options.isBlocking(), is(true));
    assertThat(options.getSources(), is(SOURCES));
    assertThat(options.getTarget(), is(TARGET));
    assertThat(options.getCredentialsProvider(), is(ConfigurationVariable.CREDENTIAL_PROVIDER.defaultURIValue()));
    assertThat(options.getMultipartUploadPartSize(),
        is(ConfigurationVariable.MINIMUM_UPLOAD_PART_SIZE.defaultLongValue()));
    assertThat(options.isS3ServerSideEncryption(),
        is(ConfigurationVariable.S3_SERVER_SIDE_ENCRYPTION.defaultBooleanValue()));
    assertThat(options.getStorageClass(), is(ConfigurationVariable.STORAGE_CLASS.defaultValue()));
    assertThat(options.getMaxBandwidth(), is(ConfigurationVariable.MAX_BANDWIDTH.defaultLongValue()));
    assertThat(options.getNumberOfUploadWorkers(),
        is(ConfigurationVariable.NUMBER_OF_UPLOAD_WORKERS.defaultIntValue()));
    assertThat(options.getMultipartUploadThreshold(),
        is(ConfigurationVariable.MULTIPART_UPLOAD_THRESHOLD.defaultLongValue()));
    assertThat(options.getMaxMaps(), is(ConfigurationVariable.MAX_MAPS.defaultIntValue()));
    assertThat(options.getCopyStrategy(), is("mystrategy"));
    assertThat(options.getLogPath(), is(nullValue()));
    assertThat(options.getRegion(), is(ConfigurationVariable.REGION.defaultValue()));
    assertThat(options.isIgnoreFailures(), is(ConfigurationVariable.IGNORE_FAILURES.defaultBooleanValue()));
    assertThat(options.getS3EndpointUri(), is(ConfigurationVariable.S3_ENDPOINT_URI.defaultURIValue()));
    assertThat(options.getUploadRetryCount(), is(ConfigurationVariable.UPLOAD_RETRY_COUNT.defaultIntValue()));
    assertThat(options.getUploadRetryDelayMs(), is(ConfigurationVariable.UPLOAD_RETRY_DELAY_MS.defaultLongValue()));
    assertThat(options.getUploadBufferSize(), is(ConfigurationVariable.UPLOAD_BUFFER_SIZE.defaultIntValue()));
    assertThat(options.getCannedAcl(), is(ConfigurationVariable.CANNED_ACL.defaultValue()));
    assertThat(options.getAssumeRole(), is(ConfigurationVariable.ASSUME_ROLE.defaultValue()));
  }

  @Test
  public void builderWithLogPath() {
    S3MapReduceCpOptions options = S3MapReduceCpOptions.builder(SOURCES, TARGET).logPath(new Path("logPath")).build();
    assertThat(options.isHelp(), is(false));
    assertThat(options.isBlocking(), is(true));
    assertThat(options.getSources(), is(SOURCES));
    assertThat(options.getTarget(), is(TARGET));
    assertThat(options.getCredentialsProvider(), is(ConfigurationVariable.CREDENTIAL_PROVIDER.defaultURIValue()));
    assertThat(options.getMultipartUploadPartSize(),
        is(ConfigurationVariable.MINIMUM_UPLOAD_PART_SIZE.defaultLongValue()));
    assertThat(options.isS3ServerSideEncryption(),
        is(ConfigurationVariable.S3_SERVER_SIDE_ENCRYPTION.defaultBooleanValue()));
    assertThat(options.getStorageClass(), is(ConfigurationVariable.STORAGE_CLASS.defaultValue()));
    assertThat(options.getMaxBandwidth(), is(ConfigurationVariable.MAX_BANDWIDTH.defaultLongValue()));
    assertThat(options.getNumberOfUploadWorkers(),
        is(ConfigurationVariable.NUMBER_OF_UPLOAD_WORKERS.defaultIntValue()));
    assertThat(options.getMultipartUploadThreshold(),
        is(ConfigurationVariable.MULTIPART_UPLOAD_THRESHOLD.defaultLongValue()));
    assertThat(options.getMaxMaps(), is(ConfigurationVariable.MAX_MAPS.defaultIntValue()));
    assertThat(options.getCopyStrategy(), is(ConfigurationVariable.COPY_STRATEGY.defaultValue()));
    assertThat(options.getLogPath(), is(new Path("logPath")));
    assertThat(options.getRegion(), is(ConfigurationVariable.REGION.defaultValue()));
    assertThat(options.isIgnoreFailures(), is(ConfigurationVariable.IGNORE_FAILURES.defaultBooleanValue()));
    assertThat(options.getS3EndpointUri(), is(ConfigurationVariable.S3_ENDPOINT_URI.defaultURIValue()));
    assertThat(options.getUploadRetryCount(), is(ConfigurationVariable.UPLOAD_RETRY_COUNT.defaultIntValue()));
    assertThat(options.getUploadRetryDelayMs(), is(ConfigurationVariable.UPLOAD_RETRY_DELAY_MS.defaultLongValue()));
    assertThat(options.getUploadBufferSize(), is(ConfigurationVariable.UPLOAD_BUFFER_SIZE.defaultIntValue()));
    assertThat(options.getCannedAcl(), is(ConfigurationVariable.CANNED_ACL.defaultValue()));
    assertThat(options.getAssumeRole(), is(ConfigurationVariable.ASSUME_ROLE.defaultValue()));
  }

  @Test
  public void builderWithRegion() {
    S3MapReduceCpOptions options = S3MapReduceCpOptions
        .builder(SOURCES, TARGET)
        .region(Regions.EU_WEST_1.getName())
        .build();
    assertThat(options.isHelp(), is(false));
    assertThat(options.isBlocking(), is(true));
    assertThat(options.getSources(), is(SOURCES));
    assertThat(options.getTarget(), is(TARGET));
    assertThat(options.getCredentialsProvider(), is(ConfigurationVariable.CREDENTIAL_PROVIDER.defaultURIValue()));
    assertThat(options.getMultipartUploadPartSize(),
        is(ConfigurationVariable.MINIMUM_UPLOAD_PART_SIZE.defaultLongValue()));
    assertThat(options.isS3ServerSideEncryption(),
        is(ConfigurationVariable.S3_SERVER_SIDE_ENCRYPTION.defaultBooleanValue()));
    assertThat(options.getStorageClass(), is(ConfigurationVariable.STORAGE_CLASS.defaultValue()));
    assertThat(options.getMaxBandwidth(), is(ConfigurationVariable.MAX_BANDWIDTH.defaultLongValue()));
    assertThat(options.getNumberOfUploadWorkers(),
        is(ConfigurationVariable.NUMBER_OF_UPLOAD_WORKERS.defaultIntValue()));
    assertThat(options.getMultipartUploadThreshold(),
        is(ConfigurationVariable.MULTIPART_UPLOAD_THRESHOLD.defaultLongValue()));
    assertThat(options.getMaxMaps(), is(ConfigurationVariable.MAX_MAPS.defaultIntValue()));
    assertThat(options.getCopyStrategy(), is(ConfigurationVariable.COPY_STRATEGY.defaultValue()));
    assertThat(options.getLogPath(), is(nullValue()));
    assertThat(options.getRegion(), is(Regions.EU_WEST_1.getName()));
    assertThat(options.isIgnoreFailures(), is(ConfigurationVariable.IGNORE_FAILURES.defaultBooleanValue()));
    assertThat(options.getS3EndpointUri(), is(ConfigurationVariable.S3_ENDPOINT_URI.defaultURIValue()));
    assertThat(options.getUploadRetryCount(), is(ConfigurationVariable.UPLOAD_RETRY_COUNT.defaultIntValue()));
    assertThat(options.getUploadRetryDelayMs(), is(ConfigurationVariable.UPLOAD_RETRY_DELAY_MS.defaultLongValue()));
    assertThat(options.getUploadBufferSize(), is(ConfigurationVariable.UPLOAD_BUFFER_SIZE.defaultIntValue()));
    assertThat(options.getCannedAcl(), is(ConfigurationVariable.CANNED_ACL.defaultValue()));
    assertThat(options.getAssumeRole(), is(ConfigurationVariable.ASSUME_ROLE.defaultValue()));
  }

  @Test
  public void builderWithIgnoreFailures() {
    S3MapReduceCpOptions options = S3MapReduceCpOptions.builder(SOURCES, TARGET).ignoreFailures(true).build();
    assertThat(options.isHelp(), is(false));
    assertThat(options.isBlocking(), is(true));
    assertThat(options.getSources(), is(SOURCES));
    assertThat(options.getTarget(), is(TARGET));
    assertThat(options.getCredentialsProvider(), is(ConfigurationVariable.CREDENTIAL_PROVIDER.defaultURIValue()));
    assertThat(options.getMultipartUploadPartSize(),
        is(ConfigurationVariable.MINIMUM_UPLOAD_PART_SIZE.defaultLongValue()));
    assertThat(options.isS3ServerSideEncryption(),
        is(ConfigurationVariable.S3_SERVER_SIDE_ENCRYPTION.defaultBooleanValue()));
    assertThat(options.getStorageClass(), is(ConfigurationVariable.STORAGE_CLASS.defaultValue()));
    assertThat(options.getMaxBandwidth(), is(ConfigurationVariable.MAX_BANDWIDTH.defaultLongValue()));
    assertThat(options.getNumberOfUploadWorkers(),
        is(ConfigurationVariable.NUMBER_OF_UPLOAD_WORKERS.defaultIntValue()));
    assertThat(options.getMultipartUploadThreshold(),
        is(ConfigurationVariable.MULTIPART_UPLOAD_THRESHOLD.defaultLongValue()));
    assertThat(options.getMaxMaps(), is(ConfigurationVariable.MAX_MAPS.defaultIntValue()));
    assertThat(options.getCopyStrategy(), is(ConfigurationVariable.COPY_STRATEGY.defaultValue()));
    assertThat(options.getLogPath(), is(nullValue()));
    assertThat(options.getRegion(), is(ConfigurationVariable.REGION.defaultValue()));
    assertThat(options.isIgnoreFailures(), is(true));
    assertThat(options.getS3EndpointUri(), is(ConfigurationVariable.S3_ENDPOINT_URI.defaultURIValue()));
    assertThat(options.getUploadRetryCount(), is(ConfigurationVariable.UPLOAD_RETRY_COUNT.defaultIntValue()));
    assertThat(options.getUploadRetryDelayMs(), is(ConfigurationVariable.UPLOAD_RETRY_DELAY_MS.defaultLongValue()));
    assertThat(options.getUploadBufferSize(), is(ConfigurationVariable.UPLOAD_BUFFER_SIZE.defaultIntValue()));
    assertThat(options.getCannedAcl(), is(ConfigurationVariable.CANNED_ACL.defaultValue()));
    assertThat(options.getAssumeRole(), is(ConfigurationVariable.ASSUME_ROLE.defaultValue()));
  }

  @Test
  public void builderWithS3EndpointUri() {
    S3MapReduceCpOptions options = S3MapReduceCpOptions
        .builder(SOURCES, TARGET)
        .s3EndpointUri(URI.create("http://s3.endpoint/"))
        .build();
    assertThat(options.isHelp(), is(false));
    assertThat(options.isBlocking(), is(true));
    assertThat(options.getSources(), is(SOURCES));
    assertThat(options.getTarget(), is(TARGET));
    assertThat(options.getCredentialsProvider(), is(ConfigurationVariable.CREDENTIAL_PROVIDER.defaultURIValue()));
    assertThat(options.getMultipartUploadPartSize(),
        is(ConfigurationVariable.MINIMUM_UPLOAD_PART_SIZE.defaultLongValue()));
    assertThat(options.isS3ServerSideEncryption(),
        is(ConfigurationVariable.S3_SERVER_SIDE_ENCRYPTION.defaultBooleanValue()));
    assertThat(options.getStorageClass(), is(ConfigurationVariable.STORAGE_CLASS.defaultValue()));
    assertThat(options.getMaxBandwidth(), is(ConfigurationVariable.MAX_BANDWIDTH.defaultLongValue()));
    assertThat(options.getNumberOfUploadWorkers(),
        is(ConfigurationVariable.NUMBER_OF_UPLOAD_WORKERS.defaultIntValue()));
    assertThat(options.getMultipartUploadThreshold(),
        is(ConfigurationVariable.MULTIPART_UPLOAD_THRESHOLD.defaultLongValue()));
    assertThat(options.getMaxMaps(), is(ConfigurationVariable.MAX_MAPS.defaultIntValue()));
    assertThat(options.getCopyStrategy(), is(ConfigurationVariable.COPY_STRATEGY.defaultValue()));
    assertThat(options.getLogPath(), is(nullValue()));
    assertThat(options.getRegion(), is(ConfigurationVariable.REGION.defaultValue()));
    assertThat(options.isIgnoreFailures(), is(ConfigurationVariable.IGNORE_FAILURES.defaultBooleanValue()));
    assertThat(options.getS3EndpointUri(), is(URI.create("http://s3.endpoint/")));
    assertThat(options.getUploadRetryCount(), is(ConfigurationVariable.UPLOAD_RETRY_COUNT.defaultIntValue()));
    assertThat(options.getUploadRetryDelayMs(), is(ConfigurationVariable.UPLOAD_RETRY_DELAY_MS.defaultLongValue()));
    assertThat(options.getUploadBufferSize(), is(ConfigurationVariable.UPLOAD_BUFFER_SIZE.defaultIntValue()));
    assertThat(options.getCannedAcl(), is(ConfigurationVariable.CANNED_ACL.defaultValue()));
    assertThat(options.getAssumeRole(), is(ConfigurationVariable.ASSUME_ROLE.defaultValue()));
  }

  @Test
  public void builderWithUploadRetryCount() {
    S3MapReduceCpOptions options = S3MapReduceCpOptions.builder(SOURCES, TARGET).uploadRetryCount(10).build();
    assertThat(options.isHelp(), is(false));
    assertThat(options.isBlocking(), is(true));
    assertThat(options.getSources(), is(SOURCES));
    assertThat(options.getTarget(), is(TARGET));
    assertThat(options.getCredentialsProvider(), is(ConfigurationVariable.CREDENTIAL_PROVIDER.defaultURIValue()));
    assertThat(options.getMultipartUploadPartSize(),
        is(ConfigurationVariable.MINIMUM_UPLOAD_PART_SIZE.defaultLongValue()));
    assertThat(options.isS3ServerSideEncryption(),
        is(ConfigurationVariable.S3_SERVER_SIDE_ENCRYPTION.defaultBooleanValue()));
    assertThat(options.getStorageClass(), is(ConfigurationVariable.STORAGE_CLASS.defaultValue()));
    assertThat(options.getMaxBandwidth(), is(ConfigurationVariable.MAX_BANDWIDTH.defaultLongValue()));
    assertThat(options.getNumberOfUploadWorkers(),
        is(ConfigurationVariable.NUMBER_OF_UPLOAD_WORKERS.defaultIntValue()));
    assertThat(options.getMultipartUploadThreshold(),
        is(ConfigurationVariable.MULTIPART_UPLOAD_THRESHOLD.defaultLongValue()));
    assertThat(options.getMaxMaps(), is(ConfigurationVariable.MAX_MAPS.defaultIntValue()));
    assertThat(options.getCopyStrategy(), is(ConfigurationVariable.COPY_STRATEGY.defaultValue()));
    assertThat(options.getLogPath(), is(nullValue()));
    assertThat(options.getRegion(), is(ConfigurationVariable.REGION.defaultValue()));
    assertThat(options.isIgnoreFailures(), is(ConfigurationVariable.IGNORE_FAILURES.defaultBooleanValue()));
    assertThat(options.getS3EndpointUri(), is(ConfigurationVariable.S3_ENDPOINT_URI.defaultURIValue()));
    assertThat(options.getUploadRetryCount(), is(10));
    assertThat(options.getUploadRetryDelayMs(), is(ConfigurationVariable.UPLOAD_RETRY_DELAY_MS.defaultLongValue()));
    assertThat(options.getUploadBufferSize(), is(ConfigurationVariable.UPLOAD_BUFFER_SIZE.defaultIntValue()));
    assertThat(options.getCannedAcl(), is(ConfigurationVariable.CANNED_ACL.defaultValue()));
    assertThat(options.getAssumeRole(), is(ConfigurationVariable.ASSUME_ROLE.defaultValue()));
  }

  @Test
  public void builderWithUploadRetryDelayMs() {
    S3MapReduceCpOptions options = S3MapReduceCpOptions.builder(SOURCES, TARGET).uploadRetryDelayMs(666L).build();
    assertThat(options.isHelp(), is(false));
    assertThat(options.isBlocking(), is(true));
    assertThat(options.getSources(), is(SOURCES));
    assertThat(options.getTarget(), is(TARGET));
    assertThat(options.getCredentialsProvider(), is(nullValue()));
    assertThat(options.getMultipartUploadPartSize(),
        is(ConfigurationVariable.MINIMUM_UPLOAD_PART_SIZE.defaultLongValue()));
    assertThat(options.isS3ServerSideEncryption(),
        is(ConfigurationVariable.S3_SERVER_SIDE_ENCRYPTION.defaultBooleanValue()));
    assertThat(options.getStorageClass(), is(ConfigurationVariable.STORAGE_CLASS.defaultValue()));
    assertThat(options.getMaxBandwidth(), is(ConfigurationVariable.MAX_BANDWIDTH.defaultLongValue()));
    assertThat(options.getNumberOfUploadWorkers(),
        is(ConfigurationVariable.NUMBER_OF_UPLOAD_WORKERS.defaultIntValue()));
    assertThat(options.getMultipartUploadThreshold(),
        is(ConfigurationVariable.MULTIPART_UPLOAD_THRESHOLD.defaultLongValue()));
    assertThat(options.getMaxMaps(), is(ConfigurationVariable.MAX_MAPS.defaultIntValue()));
    assertThat(options.getCopyStrategy(), is(ConfigurationVariable.COPY_STRATEGY.defaultValue()));
    assertThat(options.getLogPath(), is(nullValue()));
    assertThat(options.getRegion(), is(ConfigurationVariable.REGION.defaultValue()));
    assertThat(options.isIgnoreFailures(), is(ConfigurationVariable.IGNORE_FAILURES.defaultBooleanValue()));
    assertThat(options.getS3EndpointUri(), is(ConfigurationVariable.S3_ENDPOINT_URI.defaultURIValue()));
    assertThat(options.getUploadRetryCount(), is(ConfigurationVariable.UPLOAD_RETRY_COUNT.defaultIntValue()));
    assertThat(options.getUploadRetryDelayMs(), is(666L));
    assertThat(options.getUploadBufferSize(), is(ConfigurationVariable.UPLOAD_BUFFER_SIZE.defaultIntValue()));
    assertThat(options.getCannedAcl(), is(ConfigurationVariable.CANNED_ACL.defaultValue()));
    assertThat(options.getAssumeRole(), is(ConfigurationVariable.ASSUME_ROLE.defaultValue()));
  }

  @Test
  public void builderWithUploadBufferSize() {
    S3MapReduceCpOptions options = S3MapReduceCpOptions.builder(SOURCES, TARGET).uploadBufferSize(512).build();
    assertThat(options.isHelp(), is(false));
    assertThat(options.isBlocking(), is(true));
    assertThat(options.getSources(), is(SOURCES));
    assertThat(options.getTarget(), is(TARGET));
    assertThat(options.getCredentialsProvider(), is(nullValue()));
    assertThat(options.getMultipartUploadPartSize(),
        is(ConfigurationVariable.MINIMUM_UPLOAD_PART_SIZE.defaultLongValue()));
    assertThat(options.isS3ServerSideEncryption(),
        is(ConfigurationVariable.S3_SERVER_SIDE_ENCRYPTION.defaultBooleanValue()));
    assertThat(options.getStorageClass(), is(ConfigurationVariable.STORAGE_CLASS.defaultValue()));
    assertThat(options.getMaxBandwidth(), is(ConfigurationVariable.MAX_BANDWIDTH.defaultLongValue()));
    assertThat(options.getNumberOfUploadWorkers(),
        is(ConfigurationVariable.NUMBER_OF_UPLOAD_WORKERS.defaultIntValue()));
    assertThat(options.getMultipartUploadThreshold(),
        is(ConfigurationVariable.MULTIPART_UPLOAD_THRESHOLD.defaultLongValue()));
    assertThat(options.getMaxMaps(), is(ConfigurationVariable.MAX_MAPS.defaultIntValue()));
    assertThat(options.getCopyStrategy(), is(ConfigurationVariable.COPY_STRATEGY.defaultValue()));
    assertThat(options.getLogPath(), is(nullValue()));
    assertThat(options.getRegion(), is(ConfigurationVariable.REGION.defaultValue()));
    assertThat(options.isIgnoreFailures(), is(ConfigurationVariable.IGNORE_FAILURES.defaultBooleanValue()));
    assertThat(options.getS3EndpointUri(), is(ConfigurationVariable.S3_ENDPOINT_URI.defaultURIValue()));
    assertThat(options.getUploadRetryCount(), is(ConfigurationVariable.UPLOAD_RETRY_COUNT.defaultIntValue()));
    assertThat(options.getUploadRetryDelayMs(), is(ConfigurationVariable.UPLOAD_RETRY_DELAY_MS.defaultLongValue()));
    assertThat(options.getUploadBufferSize(), is(512));
    assertThat(options.getCannedAcl(), is(ConfigurationVariable.CANNED_ACL.defaultValue()));
    assertThat(options.getAssumeRole(), is(ConfigurationVariable.ASSUME_ROLE.defaultValue()));
  }

  @Test
  public void builderWithCannedAcl() {
    S3MapReduceCpOptions options = S3MapReduceCpOptions
            .builder(SOURCES, TARGET)
            .cannedAcl(CannedAccessControlList.BucketOwnerFullControl.toString())
            .build();
    assertThat(options.isHelp(), is(false));
    assertThat(options.isBlocking(), is(true));
    assertThat(options.getSources(), is(SOURCES));
    assertThat(options.getTarget(), is(TARGET));
    assertThat(options.getCredentialsProvider(), is(ConfigurationVariable.CREDENTIAL_PROVIDER.defaultURIValue()));
    assertThat(options.getMultipartUploadPartSize(),
            is(ConfigurationVariable.MINIMUM_UPLOAD_PART_SIZE.defaultLongValue()));
    assertThat(options.isS3ServerSideEncryption(),
            is(ConfigurationVariable.S3_SERVER_SIDE_ENCRYPTION.defaultBooleanValue()));
    assertThat(options.getStorageClass(), is(ConfigurationVariable.STORAGE_CLASS.defaultValue()));
    assertThat(options.getMaxBandwidth(), is(ConfigurationVariable.MAX_BANDWIDTH.defaultLongValue()));
    assertThat(options.getNumberOfUploadWorkers(),
            is(ConfigurationVariable.NUMBER_OF_UPLOAD_WORKERS.defaultIntValue()));
    assertThat(options.getMultipartUploadThreshold(),
            is(ConfigurationVariable.MULTIPART_UPLOAD_THRESHOLD.defaultLongValue()));
    assertThat(options.getMaxMaps(), is(ConfigurationVariable.MAX_MAPS.defaultIntValue()));
    assertThat(options.getCopyStrategy(), is(ConfigurationVariable.COPY_STRATEGY.defaultValue()));
    assertThat(options.getLogPath(), is(nullValue()));
    assertThat(options.getRegion(), is(ConfigurationVariable.REGION.defaultValue()));
    assertThat(options.isIgnoreFailures(), is(ConfigurationVariable.IGNORE_FAILURES.defaultBooleanValue()));
    assertThat(options.getS3EndpointUri(), is(ConfigurationVariable.S3_ENDPOINT_URI.defaultURIValue()));
    assertThat(options.getUploadRetryCount(), is(ConfigurationVariable.UPLOAD_RETRY_COUNT.defaultIntValue()));
    assertThat(options.getUploadRetryDelayMs(), is(ConfigurationVariable.UPLOAD_RETRY_DELAY_MS.defaultLongValue()));
    assertThat(options.getUploadBufferSize(), is(ConfigurationVariable.UPLOAD_BUFFER_SIZE.defaultIntValue()));
    assertThat(options.getCannedAcl(), is(CannedAccessControlList.BucketOwnerFullControl.toString()));
    assertThat(options.getAssumeRole(), is(ConfigurationVariable.ASSUME_ROLE.defaultValue()));
  }

  @Test
  public void builderWithAssumeRole() {
    S3MapReduceCpOptions options = S3MapReduceCpOptions
            .builder(SOURCES, TARGET)
            .assumeRole("iam:role:1234:user")
            .build();
    assertThat(options.isHelp(), is(false));
    assertThat(options.isBlocking(), is(true));
    assertThat(options.getSources(), is(SOURCES));
    assertThat(options.getTarget(), is(TARGET));
    assertThat(options.getCredentialsProvider(), is(ConfigurationVariable.CREDENTIAL_PROVIDER.defaultURIValue()));
    assertThat(options.getMultipartUploadPartSize(),
            is(ConfigurationVariable.MINIMUM_UPLOAD_PART_SIZE.defaultLongValue()));
    assertThat(options.isS3ServerSideEncryption(),
            is(ConfigurationVariable.S3_SERVER_SIDE_ENCRYPTION.defaultBooleanValue()));
    assertThat(options.getStorageClass(), is(ConfigurationVariable.STORAGE_CLASS.defaultValue()));
    assertThat(options.getMaxBandwidth(), is(ConfigurationVariable.MAX_BANDWIDTH.defaultLongValue()));
    assertThat(options.getNumberOfUploadWorkers(),
            is(ConfigurationVariable.NUMBER_OF_UPLOAD_WORKERS.defaultIntValue()));
    assertThat(options.getMultipartUploadThreshold(),
            is(ConfigurationVariable.MULTIPART_UPLOAD_THRESHOLD.defaultLongValue()));
    assertThat(options.getMaxMaps(), is(ConfigurationVariable.MAX_MAPS.defaultIntValue()));
    assertThat(options.getCopyStrategy(), is(ConfigurationVariable.COPY_STRATEGY.defaultValue()));
    assertThat(options.getLogPath(), is(nullValue()));
    assertThat(options.getRegion(), is(ConfigurationVariable.REGION.defaultValue()));
    assertThat(options.isIgnoreFailures(), is(ConfigurationVariable.IGNORE_FAILURES.defaultBooleanValue()));
    assertThat(options.getS3EndpointUri(), is(ConfigurationVariable.S3_ENDPOINT_URI.defaultURIValue()));
    assertThat(options.getUploadRetryCount(), is(ConfigurationVariable.UPLOAD_RETRY_COUNT.defaultIntValue()));
    assertThat(options.getUploadRetryDelayMs(), is(ConfigurationVariable.UPLOAD_RETRY_DELAY_MS.defaultLongValue()));
    assertThat(options.getUploadBufferSize(), is(ConfigurationVariable.UPLOAD_BUFFER_SIZE.defaultIntValue()));
    assertThat(options.getCannedAcl(), is(ConfigurationVariable.CANNED_ACL.defaultValue()));
    assertThat(options.getAssumeRole(), is("iam:role:1234:user"));
  }
}
