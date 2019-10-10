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
package com.hotels.bdp.circustrain.s3s3copier;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.collections.MapUtils;

import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.transfer.TransferManagerConfiguration;
import com.hotels.bdp.circustrain.aws.AssumeRoleCredentialProvider;
import com.hotels.bdp.circustrain.aws.CannedAclUtils;

public class S3S3CopierOptions {

  public static enum Keys {
    /**
     * {@link TransferManagerConfiguration#getMultipartUploadThreshold()} Default value provided by
     * TransferManagerConfiguration should be ok for most applications.
     */
    MULTIPART_COPY_THRESHOLD("s3s3-multipart-copy-threshold-in-bytes"),
    /**
     * {@link TransferManagerConfiguration#getMultipartCopyPartSize()} Default value provided by
     * TransferManagerConfiguration should be ok for most applications.
     */
    MULTIPART_COPY_PART_SIZE("s3s3-multipart-copy-part-size-in-bytes"),
    /**
     * S3 endpoint to use when creating S3 clients. To configure a specific region,
     * {@code S3_ENDPOINT_URI + "." + REGION} can be used as a copier option.
     */
    S3_ENDPOINT_URI("s3-endpoint-uri"),
    /**
     * {@link ObjectMetadata#setSSEAlgorithm(String)}
     * Whether to enable server side encryption.
     */
    S3_SERVER_SIDE_ENCRYPTION("s3-server-side-encryption"),
    /**
     * {@link com.amazonaws.services.s3.model.CannedAccessControlList}
     * Optional AWS Canned ACL
     */
    CANNED_ACL("canned-acl"),
    /**
     * Role to assume when writing S3 data
     */
    ASSUME_ROLE("assume-role"),
    /**
     * Number of copy attempts to allow when copying from S3 to S3. Default value is 3.
     */
    MAX_COPY_ATTEMPTS("s3s3-retry-max-copy-attempts");

    private final String keyName;

    Keys(String keyName) {
      this.keyName = keyName;
    }

    public String keyName() {
      return keyName;
    }
  }

  private final Map<String, Object> copierOptions;

  public S3S3CopierOptions() {
    copierOptions = new HashMap<>();
  }

  public S3S3CopierOptions(Map<String, Object> copierOptions) {
    this.copierOptions = new HashMap<>(copierOptions);
  }

  public Long getMultipartCopyThreshold() {
    return MapUtils.getLong(copierOptions, Keys.MULTIPART_COPY_THRESHOLD.keyName(), null);
  }

  public Long getMultipartCopyPartSize() {
    return MapUtils.getLong(copierOptions, Keys.MULTIPART_COPY_PART_SIZE.keyName(), null);
  }

  public URI getS3Endpoint() {
    return s3Endpoint(Keys.S3_ENDPOINT_URI.keyName());
  }

  public URI getS3Endpoint(String region) {
    URI uri = s3Endpoint(Keys.S3_ENDPOINT_URI.keyName() + "." + region);
    if (uri == null) {
      uri = s3Endpoint(Keys.S3_ENDPOINT_URI.keyName());
    }
    return uri;
  }

  private URI s3Endpoint(String keyName) {
    String endpoint = MapUtils.getString(copierOptions, keyName, null);
    if (endpoint == null) {
      return null;
    }
    return URI.create(endpoint);
  }

  public Boolean isS3ServerSideEncryption() {
    return MapUtils.getBoolean(copierOptions, Keys.S3_SERVER_SIDE_ENCRYPTION.keyName(), false);
  }

  public CannedAccessControlList getCannedAcl() {
     String cannedAcl = MapUtils.getString(copierOptions, Keys.CANNED_ACL.keyName(), null);
     if (cannedAcl != null) {
       return CannedAclUtils.toCannedAccessControlList(cannedAcl);
     }

     return null;
  }
  
  // *
  // * need roles for source and target ?????
  public String getAssumedRole() {
    String role = MapUtils.getString(copierOptions, Keys.ASSUME_ROLE.keyName(), null);
    if (role != null) {
      return role;
    }
    return null;
  }
  
  public String getSourceAssumedRole() {
    // what is the key name?
    return null;
  }

  public String getTargetAssumedRole() {
    // what is the key name?
    return null;
  }

  public int getMaxCopyAttempts() {
    Integer maxCopyAttempts = MapUtils.getInteger(copierOptions, Keys.MAX_COPY_ATTEMPTS.keyName(), 3);
    return maxCopyAttempts < 1 ? 3 : maxCopyAttempts;
  }
}
