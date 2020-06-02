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

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.DeleteObjectsRequest.KeyVersion;
import com.amazonaws.services.s3.model.DeleteObjectsResult;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import com.hotels.bdp.circustrain.api.conf.DataManipulationClient;

public class AwsDataManipulationClient implements DataManipulationClient {

  private static final Logger LOG = LoggerFactory.getLogger(AwsDataManipulationClient.class);

  private AmazonS3 s3Client;

  public AwsDataManipulationClient(AmazonS3 s3Client) {
    this.s3Client = s3Client;
  }

  @Override
  public boolean delete(String path) {
    if (path.toLowerCase().startsWith("s3://")) {
      LOG.info("Deleting all data at location: {}", path);
      AmazonS3URI uri = new AmazonS3URI(path);
      String bucket = uri.getBucket();

      List<KeyVersion> keysToDelete = getKeysToDelete(bucket, uri.getKey());
      LOG.debug("Deleting keys: {}", keysToDelete.toString());

      DeleteObjectsResult result = s3Client.deleteObjects(new DeleteObjectsRequest(bucket).withKeys(keysToDelete));
      return successfulDeletion(result, keysToDelete.size());
    }
    LOG.info("Can't delete data at location: {} using AWS client", path);
    return false;
  }

  private List<KeyVersion> getKeysToDelete(String bucket, String key) {
    return getKeysFromListing(s3Client.listObjects(bucket, key));
  }

  private List<KeyVersion> getKeysFromListing(ObjectListing listing) {
    Set<String> keys = listing.getObjectSummaries().stream().map(S3ObjectSummary::getKey).collect(Collectors.toSet());
    List<KeyVersion> keysToDelete = keys.stream().map(k -> new KeyVersion(k)).collect(Collectors.toList());

    if (!listing.isTruncated()) {
      return keysToDelete;
    }
    listing.setNextMarker(listing.getNextMarker());
    keysToDelete.addAll(getKeysFromListing(listing));

    return keysToDelete;
  }

  private boolean successfulDeletion(DeleteObjectsResult result, int countToDelete) {
    int amountDeleted = result.getDeletedObjects().size();
    return amountDeleted == countToDelete;
  }

}
