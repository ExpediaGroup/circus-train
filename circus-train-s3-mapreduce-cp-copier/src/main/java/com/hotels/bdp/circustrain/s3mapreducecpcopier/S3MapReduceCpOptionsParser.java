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
package com.hotels.bdp.circustrain.s3mapreducecpcopier;

import java.net.URI;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.MapUtils;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hotels.bdp.circustrain.core.util.MoreMapUtils;
import com.hotels.bdp.circustrain.s3mapreducecp.ConfigurationVariable;
import com.hotels.bdp.circustrain.s3mapreducecp.S3MapReduceCpOptions;

public class S3MapReduceCpOptionsParser {

  private static final Logger LOG = LoggerFactory.getLogger(S3MapReduceCpOptionsParser.class);

  public static final String CREDENTIAL_PROVIDER = "credential-provider";
  public static final String TASK_BANDWIDTH = "task-bandwidth";
  public static final String STORAGE_CLASS = "storage-class";
  public static final String S3_SERVER_SIDE_ENCRYPTION = "s3-server-side-encryption";
  public static final String MULTIPART_UPLOAD_CHUNK_SIZE = "multipart-upload-chunk-size";
  public static final String MULTIPART_UPLOAD_THRESHOLD = "multipart-upload-threshold";
  public static final String MAX_MAPS = "max-maps";
  public static final String NUMBER_OF_WORKERS_PER_MAP = "num-of-workers-per-map";
  public static final String COPY_STRATEGY = "copy-strategy";
  public static final String LOG_PATH = "log-path";
  public static final String REGION = "region";
  public static final String IGNORE_FAILURES = "ignore-failures";
  public static final String S3_ENDPOINT_URI = "s3-endpoint-uri";
  public static final String UPLOAD_RETRY_COUNT = "upload-retry-count";
  public static final String UPLOAD_RETRY_DELAY_MS = "upload-retry-delay-ms";
  public static final String UPLOAD_BUFFER_SIZE = "upload-buffer-size";
  public static final String CANNED_ACL = "canned-acl";

  private final S3MapReduceCpOptions.Builder optionsBuilder;
  private final URI defaultCredentialsProvider;

  S3MapReduceCpOptionsParser(List<Path> sources, URI target, URI defaultCredentialsProvider) {
    this.defaultCredentialsProvider = defaultCredentialsProvider;
    optionsBuilder = S3MapReduceCpOptions.builder(sources, target).blocking(false);
  }

  protected S3MapReduceCpOptions parse(Map<String, Object> copierOptions) {
    if (copierOptions == null) {
      LOG.debug("Null copier options: nothing to parse");
      return optionsBuilder.build();
    }

    optionsBuilder
        .credentialsProvider(MoreMapUtils.getUri(copierOptions, CREDENTIAL_PROVIDER, defaultCredentialsProvider));

    long multipartUploadPartSize = MapUtils.getLongValue(copierOptions, MULTIPART_UPLOAD_CHUNK_SIZE,
        ConfigurationVariable.MULTIPART_UPLOAD_THRESHOLD.defaultLongValue());
    if (multipartUploadPartSize <= 0) {
      throw new IllegalArgumentException("Parameter " + MULTIPART_UPLOAD_CHUNK_SIZE + " must be greater than zero");
    }
    optionsBuilder.multipartUploadPartSize(multipartUploadPartSize);

    optionsBuilder.s3ServerSideEncryption(MapUtils.getBoolean(copierOptions, S3_SERVER_SIDE_ENCRYPTION, true));

    optionsBuilder.storageClass(
        MapUtils.getString(copierOptions, STORAGE_CLASS, ConfigurationVariable.STORAGE_CLASS.defaultValue()));

    long maxBandwidth = MapUtils.getLongValue(copierOptions, TASK_BANDWIDTH,
        ConfigurationVariable.MAX_BANDWIDTH.defaultLongValue());
    if (maxBandwidth <= 0) {
      throw new IllegalArgumentException(
          "Parameter " + TASK_BANDWIDTH + " must be a positive number greater then zero");
    }
    optionsBuilder.maxBandwidth(maxBandwidth);

    int numberOfUploadWorkers = MapUtils.getIntValue(copierOptions, NUMBER_OF_WORKERS_PER_MAP,
        ConfigurationVariable.NUMBER_OF_UPLOAD_WORKERS.defaultIntValue());
    if (numberOfUploadWorkers <= 0) {
      throw new IllegalArgumentException(
          "Parameter " + NUMBER_OF_WORKERS_PER_MAP + " must be a positive number greater than zero");
    }
    optionsBuilder.numberOfUploadWorkers(numberOfUploadWorkers);

    long multipartUploadThreshold = MapUtils.getLongValue(copierOptions, MULTIPART_UPLOAD_THRESHOLD,
        ConfigurationVariable.MULTIPART_UPLOAD_THRESHOLD.defaultLongValue());
    if (multipartUploadThreshold <= 0) {
      throw new IllegalArgumentException("Parameter " + MULTIPART_UPLOAD_THRESHOLD + " must be greater than zero");
    }
    optionsBuilder.multipartUploadThreshold(multipartUploadThreshold);

    int maxMaps = MapUtils.getIntValue(copierOptions, MAX_MAPS, ConfigurationVariable.MAX_MAPS.defaultIntValue());
    if (maxMaps <= 0) {
      throw new IllegalArgumentException("Parameter " + MAX_MAPS + " must be a positive number greater than zero");
    }
    optionsBuilder.maxMaps(maxMaps);

    optionsBuilder.copyStrategy(
        MapUtils.getString(copierOptions, COPY_STRATEGY, ConfigurationVariable.COPY_STRATEGY.defaultValue()));

    Path logPath = MoreMapUtils.getHadoopPath(copierOptions, LOG_PATH, null);
    if (logPath != null) {
      optionsBuilder.logPath(logPath);
    }

    optionsBuilder.region(MapUtils.getString(copierOptions, REGION, ConfigurationVariable.REGION.defaultValue()));

    optionsBuilder.ignoreFailures(MapUtils.getBoolean(copierOptions, IGNORE_FAILURES,
        ConfigurationVariable.IGNORE_FAILURES.defaultBooleanValue()));

    optionsBuilder.s3EndpointUri(
        MoreMapUtils.getUri(copierOptions, S3_ENDPOINT_URI, ConfigurationVariable.S3_ENDPOINT_URI.defaultURIValue()));

    int uploadRetryCount = MapUtils.getInteger(copierOptions, UPLOAD_RETRY_COUNT,
        ConfigurationVariable.UPLOAD_RETRY_COUNT.defaultIntValue());
    if (uploadRetryCount < 0) {
      throw new IllegalArgumentException("Parameter " + UPLOAD_RETRY_COUNT + " must be a positive number");
    }
    optionsBuilder.uploadRetryCount(uploadRetryCount);

    long uploadRetryDelaysMs = MapUtils.getLong(copierOptions, UPLOAD_RETRY_DELAY_MS,
        ConfigurationVariable.UPLOAD_RETRY_DELAY_MS.defaultLongValue());
    optionsBuilder.uploadRetryDelayMs(uploadRetryDelaysMs);
    if (uploadRetryDelaysMs < 0) {
      throw new IllegalArgumentException("Parameter " + UPLOAD_RETRY_DELAY_MS + " must be a positive number");
    }

    int uploadBufferSize = MapUtils.getInteger(copierOptions, UPLOAD_BUFFER_SIZE,
        ConfigurationVariable.UPLOAD_BUFFER_SIZE.defaultIntValue());
    if (uploadBufferSize < 0) {
      throw new IllegalArgumentException("Parameter " + UPLOAD_BUFFER_SIZE + " must be a positive number");
    }
    optionsBuilder.uploadBufferSize(uploadBufferSize);

    optionsBuilder.cannedAcl(MapUtils.getString(copierOptions, CANNED_ACL, ConfigurationVariable.CANNED_ACL.defaultValue()));

    return optionsBuilder.build();
  }

}
