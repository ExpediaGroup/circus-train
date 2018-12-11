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
package com.hotels.bdp.circustrain.s3mapreducecp;

import java.net.URI;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;

import com.amazonaws.services.s3.model.Region;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.converters.URIConverter;
import com.beust.jcommander.validators.PositiveInteger;
import com.google.common.collect.ImmutableMap;

import com.hotels.bdp.circustrain.s3mapreducecp.aws.AwsUtil;
import com.hotels.bdp.circustrain.s3mapreducecp.jcommander.PathConverter;
import com.hotels.bdp.circustrain.s3mapreducecp.jcommander.PositiveLong;
import com.hotels.bdp.circustrain.s3mapreducecp.jcommander.PositiveNonZeroInteger;
import com.hotels.bdp.circustrain.s3mapreducecp.jcommander.PositiveNonZeroLong;
import com.hotels.bdp.circustrain.s3mapreducecp.jcommander.RegionValidator;
import com.hotels.bdp.circustrain.s3mapreducecp.jcommander.StorageClassValidator;

@Parameters(separators = "=")
public class S3MapReduceCpOptions {

  // As we shade the AWS SDK, we can't expose AWS classes in the public classes/methods.
  public static class Builder {
    private final S3MapReduceCpOptions options;

    private Builder(List<Path> sources, URI target) {
      options = new S3MapReduceCpOptions();
      options.setSources(sources);
      options.setTarget(target);
    }

    public Builder blocking(boolean blocking) {
      options.setBlocking(blocking);
      return this;
    }

    public Builder credentialsProvider(URI credentialsProvider) {
      options.setCredentialsProvider(credentialsProvider);
      return this;
    }

    public Builder multipartUploadPartSize(long multipartUploadPartSize) {
      options.setMultipartUploadPartSize(multipartUploadPartSize);
      return this;
    }

    public Builder s3ServerSideEncryption(boolean s3ServerSideEncryption) {
      options.setS3ServerSideEncryption(s3ServerSideEncryption);
      return this;
    }

    public Builder storageClass(String storageClass) {
      options.setStorageClass(storageClass);
      return this;
    }

    public Builder maxBandwidth(long maxBandwidth) {
      options.setMaxBandwidth(maxBandwidth);
      return this;
    }

    public Builder numberOfUploadWorkers(int numberOfUploadWorkers) {
      options.setNumberOfUploadWorkers(numberOfUploadWorkers);
      return this;
    }

    public Builder multipartUploadThreshold(long multipartUploadThreshold) {
      options.setMultipartUploadThreshold(multipartUploadThreshold);
      return this;
    }

    public Builder maxMaps(int maxMaps) {
      options.setMaxMaps(maxMaps);
      return this;
    }

    public Builder copyStrategy(String copyStrategy) {
      options.setCopyStrategy(copyStrategy);
      return this;
    }

    public Builder logPath(Path logPath) {
      options.setLogPath(logPath);
      return this;
    }

    public Builder region(String region) {
      options.setRegion(region);
      return this;
    }

    public Builder ignoreFailures(boolean ignoreFailures) {
      options.setIgnoreFailures(ignoreFailures);
      return this;
    }

    public Builder s3EndpointUri(URI s3EndpointUri) {
      options.setS3EndpointUri(s3EndpointUri);
      return this;
    }

    public Builder uploadRetryCount(int uploadRetryCount) {
      options.setUploadRetryCount(uploadRetryCount);
      return this;
    }

    public Builder uploadRetryDelayMs(long uploadRetryDelayMs) {
      options.setUploadRetryDelayMs(uploadRetryDelayMs);
      return this;
    }

    public Builder uploadBufferSize(int uploadBufferSize) {
      options.setUploadBufferSize(uploadBufferSize);
      return this;
    }

    public S3MapReduceCpOptions build() {
      return options;
    }
  }

  public static Builder builder(List<Path> sources, URI target) {
    return new Builder(sources, target);
  }

  @Parameter(names = "--help", description = "Print help text", help = true)
  private boolean help = false;

  @Parameter(names = "--async", description = "Should S3MapReduceCp execution be blocking")
  private boolean async = false;

  @Parameter(names = "--src", description = "Locations to copy files from", required = true,
      converter = PathConverter.class)
  private List<Path> sources = null;

  @Parameter(names = "--dest", description = "Location to copy files to", required = true,
      converter = URIConverter.class)
  private URI target = null;

  @Parameter(names = "--credentialsProvider", description = "Credentials provider URI", converter = URIConverter.class)
  private URI credentialsProvider = ConfigurationVariable.CREDENTIAL_PROVIDER.defaultURIValue();

  @Parameter(names = "--multipartUploadChunkSize", description = "The size in MB of the multipart upload part size",
      validateWith = PositiveNonZeroLong.class)
  private long multipartUploadPartSize = ConfigurationVariable.MINIMUM_UPLOAD_PART_SIZE.defaultLongValue();

  @Parameter(names = "--s3ServerSideEncryption",
      description = "Copy files to S3 using Amazon S3 Server Side Encryption")
  private boolean s3ServerSideEncryption = ConfigurationVariable.S3_SERVER_SIDE_ENCRYPTION.defaultBooleanValue();

  @Parameter(names = "--storageClass",
      description = "S3 storage class. See IDs in com.amazonaws.services.s3.model.StorageClass",
      validateWith = StorageClassValidator.class)
  private String storageClass = ConfigurationVariable.STORAGE_CLASS.defaultValue();

  @Parameter(names = "--maxBandwidth", description = "Maximum bandwidth per task specified in MB",
      validateWith = PositiveNonZeroLong.class)
  private long maxBandwidth = ConfigurationVariable.MAX_BANDWIDTH.defaultLongValue();

  @Parameter(names = "--numberOfUploadWorkers", description = "Number of threads that perform uploads to S3",
      validateWith = PositiveNonZeroInteger.class)
  private int numberOfUploadWorkers = ConfigurationVariable.NUMBER_OF_UPLOAD_WORKERS.defaultIntValue();

  @Parameter(names = "--multipartUploadThreshold", description = "Multipart upload threshold in MB",
      validateWith = PositiveNonZeroLong.class)
  private long multipartUploadThreshold = ConfigurationVariable.MULTIPART_UPLOAD_THRESHOLD.defaultLongValue();

  @Parameter(names = "--maxMaps", description = "Maximum number of Hadoop mappers",
      validateWith = PositiveInteger.class)
  private int maxMaps = ConfigurationVariable.MAX_MAPS.defaultIntValue();

  @Parameter(names = "--copyStrategy", description = "Strategy to distribute the files to be copied for each mapper")
  private String copyStrategy = ConfigurationVariable.COPY_STRATEGY.defaultValue();

  @Parameter(names = "--logPath", description = "Folder on DFS where S3MapReduceCp execution logs are saved",
      converter = PathConverter.class)
  private Path logPath = null;

  @Parameter(names = "--region", description = "AWS region", validateWith = RegionValidator.class)
  private String region = ConfigurationVariable.REGION.defaultValue();

  @Parameter(names = "--ignoreFailures", description = "Ignore read failures")
  private boolean ignoreFailures = ConfigurationVariable.IGNORE_FAILURES.defaultBooleanValue();

  @Parameter(names = "--s3EndpointUri", description = "URI of the S3 end-point to be used by S3 clients",
      converter = URIConverter.class)
  private URI s3EndpointUri = ConfigurationVariable.S3_ENDPOINT_URI.defaultURIValue();

  @Parameter(names = "--uploadRetryCount", description = "Number of upload retries",
      validateWith = PositiveInteger.class)
  private int uploadRetryCount = ConfigurationVariable.UPLOAD_RETRY_COUNT.defaultIntValue();

  @Parameter(names = "--uploadRetryDelayMs", description = "Milliseconds between upload retries",
      validateWith = PositiveLong.class)
  private long uploadRetryDelayMs = ConfigurationVariable.UPLOAD_RETRY_DELAY_MS.defaultLongValue();

  @Parameter(names = "--uploadBufferSize", description = "Size of the buffer used to upload the stream of data",
      validateWith = PositiveInteger.class)
  private int uploadBufferSize = ConfigurationVariable.UPLOAD_BUFFER_SIZE.defaultIntValue();

  public S3MapReduceCpOptions() {}

  public S3MapReduceCpOptions(S3MapReduceCpOptions options) {
    help = options.help;
    async = options.async;
    sources = options.sources;
    target = options.target;
    credentialsProvider = options.credentialsProvider;
    multipartUploadPartSize = options.multipartUploadPartSize;
    s3ServerSideEncryption = options.s3ServerSideEncryption;
    storageClass = options.storageClass;
    maxBandwidth = options.maxBandwidth;
    numberOfUploadWorkers = options.numberOfUploadWorkers;
    multipartUploadThreshold = options.multipartUploadThreshold;
    maxMaps = options.maxMaps;
    copyStrategy = options.copyStrategy;
    logPath = options.logPath;
    region = options.region;
    ignoreFailures = options.ignoreFailures;
    s3EndpointUri = options.s3EndpointUri;
    uploadRetryCount = options.uploadRetryCount;
    uploadRetryDelayMs = options.uploadRetryDelayMs;
    uploadBufferSize = options.uploadBufferSize;
  }

  public boolean isHelp() {
    return help;
  }

  void setHelp(boolean help) {
    this.help = help;
  }

  public boolean isBlocking() {
    return !async;
  }

  void setBlocking(boolean blocking) {
    async = !blocking;
  }

  public List<Path> getSources() {
    return sources;
  }

  void setSources(List<Path> sources) {
    this.sources = sources;
  }

  public URI getTarget() {
    return target;
  }

  void setTarget(URI target) {
    this.target = target;
  }

  public URI getCredentialsProvider() {
    return credentialsProvider;
  }

  void setCredentialsProvider(URI credentialsProvider) {
    this.credentialsProvider = credentialsProvider;
  }

  public long getMultipartUploadPartSize() {
    return multipartUploadPartSize;
  }

  void setMultipartUploadPartSize(long multipartUploadPartSize) {
    this.multipartUploadPartSize = multipartUploadPartSize;
  }

  public boolean isS3ServerSideEncryption() {
    return s3ServerSideEncryption;
  }

  void setS3ServerSideEncryption(boolean s3ServerSideEncryption) {
    this.s3ServerSideEncryption = s3ServerSideEncryption;
  }

  public String getStorageClass() {
    return storageClass.toString();
  }

  void setStorageClass(String storageClass) {
    storageClass = storageClass.toUpperCase();
    AwsUtil.toStorageClass(storageClass);
    this.storageClass = storageClass;
  }

  public long getMaxBandwidth() {
    return maxBandwidth;
  }

  void setMaxBandwidth(long maxBandwidth) {
    this.maxBandwidth = maxBandwidth;
  }

  public int getNumberOfUploadWorkers() {
    return numberOfUploadWorkers;
  }

  void setNumberOfUploadWorkers(int numberOfUploadWorkers) {
    this.numberOfUploadWorkers = numberOfUploadWorkers;
  }

  public long getMultipartUploadThreshold() {
    return multipartUploadThreshold;
  }

  void setMultipartUploadThreshold(long multipartUploadThreshold) {
    this.multipartUploadThreshold = multipartUploadThreshold;
  }

  public int getMaxMaps() {
    return maxMaps;
  }

  void setMaxMaps(int maxMaps) {
    this.maxMaps = maxMaps;
  }

  public String getCopyStrategy() {
    return copyStrategy;
  }

  void setCopyStrategy(String copyStrategy) {
    this.copyStrategy = copyStrategy;
  }

  public Path getLogPath() {
    return logPath;
  }

  void setLogPath(Path logPath) {
    this.logPath = logPath;
  }

  public String getRegion() {
    return region;
  }

  void setRegion(String region) {
    Region.fromValue(region);
    this.region = region;
  }

  public boolean isIgnoreFailures() {
    return ignoreFailures;
  }

  void setIgnoreFailures(boolean ignoreFailures) {
    this.ignoreFailures = ignoreFailures;
  }

  public URI getS3EndpointUri() {
    return s3EndpointUri;
  }

  void setS3EndpointUri(URI s3EndpointUri) {
    this.s3EndpointUri = s3EndpointUri;
  }

  public int getUploadRetryCount() {
    return uploadRetryCount;
  }

  public void setUploadRetryCount(int uploadRetryCount) {
    this.uploadRetryCount = uploadRetryCount;
  }

  public long getUploadRetryDelayMs() {
    return uploadRetryDelayMs;
  }

  public void setUploadRetryDelayMs(long uploadRetryDelayMs) {
    this.uploadRetryDelayMs = uploadRetryDelayMs;
  }

  public int getUploadBufferSize() {
    return uploadBufferSize;
  }

  public void setUploadBufferSize(int uploadBufferSize) {
    this.uploadBufferSize = uploadBufferSize;
  }

  public Map<String, String> toMap() {
    ImmutableMap.Builder<String, String> builder = ImmutableMap
        .<String, String>builder()
        .put(ConfigurationVariable.MINIMUM_UPLOAD_PART_SIZE.getName(), String.valueOf(multipartUploadPartSize))
        .put(ConfigurationVariable.S3_SERVER_SIDE_ENCRYPTION.getName(), String.valueOf(s3ServerSideEncryption))
        .put(ConfigurationVariable.STORAGE_CLASS.getName(), storageClass)
        .put(ConfigurationVariable.MAX_BANDWIDTH.getName(), String.valueOf(maxBandwidth))
        .put(ConfigurationVariable.NUMBER_OF_UPLOAD_WORKERS.getName(), String.valueOf(numberOfUploadWorkers))
        .put(ConfigurationVariable.MULTIPART_UPLOAD_THRESHOLD.getName(), String.valueOf(multipartUploadThreshold))
        .put(ConfigurationVariable.MAX_MAPS.getName(), String.valueOf(maxMaps))
        .put(ConfigurationVariable.COPY_STRATEGY.getName(), copyStrategy)
        .put(ConfigurationVariable.IGNORE_FAILURES.getName(), String.valueOf(ignoreFailures))
        .put(ConfigurationVariable.UPLOAD_RETRY_COUNT.getName(), String.valueOf(uploadRetryCount))
        .put(ConfigurationVariable.UPLOAD_RETRY_DELAY_MS.getName(), String.valueOf(uploadRetryDelayMs))
        .put(ConfigurationVariable.UPLOAD_BUFFER_SIZE.getName(), String.valueOf(uploadBufferSize));
    if (credentialsProvider != null) {
      builder.put(ConfigurationVariable.CREDENTIAL_PROVIDER.getName(), credentialsProvider.toString());
    }
    if (region != null) {
      builder.put(ConfigurationVariable.REGION.getName(), region);
    }
    if (s3EndpointUri != null) {
      builder.put(ConfigurationVariable.S3_ENDPOINT_URI.getName(), s3EndpointUri.toString());
    }
    return builder.build();
  }

}
