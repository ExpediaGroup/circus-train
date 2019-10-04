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

import static com.hotels.bdp.circustrain.s3mapreducecp.Constants.DEFAULT_TRANSFER_MANAGER_CONFIGURATION;

import java.net.URI;

import org.apache.hadoop.fs.Path;

import com.amazonaws.services.s3.model.StorageClass;
import com.amazonaws.services.s3.transfer.TransferManagerConfiguration;

import com.hotels.bdp.circustrain.aws.AssumeRoleCredentialProvider;

final class Constants {
  static final TransferManagerConfiguration DEFAULT_TRANSFER_MANAGER_CONFIGURATION = new TransferManagerConfiguration();

  private Constants() {}
}

public enum ConfigurationVariable {

  ASSUME_ROLE(AssumeRoleCredentialProvider.ASSUME_ROLE_PROPERTY_NAME, null),
  CANNED_ACL("com.hotels.bdp.circustrain.s3mapreducecp.cannedAcl", null),
  CREDENTIAL_PROVIDER("com.hotels.bdp.circustrain.s3mapreducecp.credentialsProvider", null),
  MINIMUM_UPLOAD_PART_SIZE("com.hotels.bdp.circustrain.s3mapreducecp.minimumUploadPartSize",
      String.valueOf(DEFAULT_TRANSFER_MANAGER_CONFIGURATION.getMinimumUploadPartSize())),
  MULTIPART_UPLOAD_THRESHOLD("com.hotels.bdp.circustrain.s3mapreducecp.multipartUploadThreshold",
      String.valueOf(DEFAULT_TRANSFER_MANAGER_CONFIGURATION.getMultipartUploadThreshold())),
  S3_SERVER_SIDE_ENCRYPTION("com.hotels.bdp.circustrain.s3mapreducecp.s3ServerSideEncryption",
      Boolean.FALSE.toString()),
  STORAGE_CLASS("com.hotels.bdp.circustrain.s3mapreducecp.storageClass", StorageClass.Standard.toString()),
  REGION("com.hotels.bdp.circustrain.s3mapreducecp.region", null),
  MAX_BANDWIDTH("com.hotels.bdp.circustrain.s3mapreducecp.maxBandwidth",
      String.valueOf(S3MapReduceCpConstants.DEFAULT_BANDWIDTH_MB)),
  NUMBER_OF_UPLOAD_WORKERS("com.hotels.bdp.circustrain.s3mapreducecp.numberOfUploadWorkers",
      String.valueOf(S3MapReduceCpConstants.DEFAULT_NUM_OF_UPLOAD_WORKERS)),
  MAX_MAPS("com.hotels.bdp.circustrain.s3mapreducecp.maxMaps", String.valueOf(S3MapReduceCpConstants.DEFAULT_MAPS)),
  COPY_STRATEGY("com.hotels.bdp.circustrain.s3mapreducecp.copyStrategy", S3MapReduceCpConstants.UNIFORMSIZE),
  IGNORE_FAILURES("com.hotels.bdp.circustrain.s3mapreducecp.ignoreFailures", Boolean.FALSE.toString()),
  S3_ENDPOINT_URI("com.hotels.bdp.circustrain.s3mapreducecp.s3EndpointUri", null),
  UPLOAD_RETRY_COUNT("com.hotels.bdp.circustrain.s3mapreducecp.uploadRetryCount",
      String.valueOf(S3MapReduceCpConstants.DEFAULT_UPLOAD_RETRIES)),
  UPLOAD_RETRY_DELAY_MS("com.hotels.bdp.circustrain.s3mapreducecp.uploadRetryDelayMs",
      String.valueOf(S3MapReduceCpConstants.DEFAULT_UPLOAD_RETRY_DELAY_MS)),
  /** Use {@code 0} to use the default FileSystem behaviour */
  UPLOAD_BUFFER_SIZE("com.hotels.bdp.circustrain.s3mapreducecp.uploadBufferSize",
      String.valueOf(S3MapReduceCpConstants.DEFAULT_UPLOAD_BUFFER_SIZE));

  private final String name;
  private final String defaultValue;

  private ConfigurationVariable(String name, String defaultValue) {
    this.name = name;
    this.defaultValue = defaultValue;
  }

  public String getName() {
    return name;
  }

  public String defaultValue() {
    return defaultValue;
  }

  public int defaultIntValue() {
    return Integer.parseInt(defaultValue);
  }

  public long defaultLongValue() {
    return Long.parseLong(defaultValue);
  }

  public boolean defaultBooleanValue() {
    return Boolean.valueOf(defaultValue);
  }

  public Path defaultPathValue() {
    return defaultValue == null ? null : new Path(defaultValue);
  }

  public URI defaultURIValue() {
    return defaultValue == null ? null : URI.create(defaultValue);
  }

}
