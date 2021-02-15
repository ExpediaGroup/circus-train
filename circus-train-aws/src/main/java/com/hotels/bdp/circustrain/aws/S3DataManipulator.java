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

import static com.hotels.bdp.circustrain.aws.AmazonS3URIs.toAmazonS3URI;

import java.net.URI;
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

import com.hotels.bdp.circustrain.api.data.DataManipulator;

public class S3DataManipulator implements DataManipulator {

  private static final Logger log = LoggerFactory.getLogger(S3DataManipulator.class);

  private AmazonS3 s3Client;

  public S3DataManipulator(AmazonS3 s3Client) {
    this.s3Client = s3Client;
  }

  @Override
  public boolean delete(String path) {
    log.info("Deleting all data at location: {}", path);
    AmazonS3URI uri = toAmazonS3URI(URI.create(path));
    String bucket = uri.getBucket();
    List<KeyVersion> keysToDelete = getKeysToDelete(bucket, uri.getKey());

    if (keysToDelete.isEmpty()) {
      log.info("Nothing to delete at location: {}", path);
      return false;
    }

    log.debug("Deleting keys: {}", keysToDelete.stream().map(k -> k.getKey()).collect(Collectors.toList()));
    DeleteObjectsResult result = s3Client.deleteObjects(new DeleteObjectsRequest(bucket).withKeys(keysToDelete));
    return successfulDeletion(result, keysToDelete.size());
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
