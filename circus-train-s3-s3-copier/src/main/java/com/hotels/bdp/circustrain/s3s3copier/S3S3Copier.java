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
package com.hotels.bdp.circustrain.s3s3copier;

import static com.hotels.bdp.circustrain.s3s3copier.aws.AmazonS3URIs.toAmazonS3URI;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.transfer.Copy;
import com.amazonaws.services.s3.transfer.Transfer;
import com.amazonaws.services.s3.transfer.Transfer.TransferState;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.internal.TransferStateChangeListener;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;

import com.hotels.bdp.circustrain.api.CircusTrainException;
import com.hotels.bdp.circustrain.api.copier.Copier;
import com.hotels.bdp.circustrain.api.metrics.Metrics;
import com.hotels.bdp.circustrain.s3s3copier.aws.AmazonS3ClientFactory;
import com.hotels.bdp.circustrain.s3s3copier.aws.ListObjectsRequestFactory;
import com.hotels.bdp.circustrain.s3s3copier.aws.TransferManagerFactory;

public class S3S3Copier implements Copier {

  private final static Logger LOG = LoggerFactory.getLogger(S3S3Copier.class);

  private static class BytesTransferStateChangeListener implements TransferStateChangeListener {

    private final S3ObjectSummary s3ObjectSummary;
    private final AmazonS3URI targetS3Uri;
    private final String targetKey;

    public BytesTransferStateChangeListener(
        S3ObjectSummary s3ObjectSummary,
        AmazonS3URI targetS3Uri,
        String targetKey) {
      this.s3ObjectSummary = s3ObjectSummary;
      this.targetS3Uri = targetS3Uri;
      this.targetKey = targetKey;
    }

    @Override
    public void transferStateChanged(Transfer transfer, TransferState state) {
      if (state == TransferState.Completed) {
        // NOTE: running progress doesn't seem to be reported correctly.
        // transfer.getProgress().getBytesTransferred() is always 0. Unsure what is the cause of this at this moment
        // so just printing total bytes when completed.
        LOG
            .debug("copied object from '{}/{}' to '{}/{}': {} bytes transferred", s3ObjectSummary.getBucketName(),
                s3ObjectSummary.getKey(), targetS3Uri.getBucket(), targetKey,
                transfer.getProgress().getTotalBytesToTransfer());
      }
    }
  }

  private static class AtomicLongGauge implements Gauge<Long> {

    private final AtomicLong value;

    public AtomicLongGauge(AtomicLong value) {
      this.value = value;
    }

    @Override
    public Long getValue() {
      return value.get();
    }
  }

  private final Path sourceBaseLocation;
  private final List<Path> sourceSubLocations;
  private final Path replicaLocation;
  private final ListObjectsRequestFactory listObjectsRequestFactory;
  private final MetricRegistry registry;
  private final AmazonS3ClientFactory s3ClientFactory;
  private final TransferManagerFactory transferManagerFactory;
  private final S3S3CopierOptions s3s3CopierOptions;

  private TransferManager transferManager;
  private final List<Copy> copyJobs = new ArrayList<>();

  private long totalBytesToReplicate = 0;
  private AmazonS3 targetClient;

  private AmazonS3 srcClient;

  public S3S3Copier(
      Path sourceBaseLocation,
      List<Path> sourceSubLocations,
      Path replicaLocation,
      AmazonS3ClientFactory s3ClientFactory,
      TransferManagerFactory transferManagerFactory,
      ListObjectsRequestFactory listObjectsRequestFactory,
      MetricRegistry registry,
      S3S3CopierOptions s3s3CopierOptions) {
    this.sourceBaseLocation = sourceBaseLocation;
    this.sourceSubLocations = sourceSubLocations;
    this.replicaLocation = replicaLocation;
    this.s3ClientFactory = s3ClientFactory;
    this.transferManagerFactory = transferManagerFactory;
    this.listObjectsRequestFactory = listObjectsRequestFactory;
    this.registry = registry;
    this.s3s3CopierOptions = s3s3CopierOptions;
  }

  @Override
  public Metrics copy() throws CircusTrainException {
    try {
      try {
        startAllCopyJobs();
        return gatherAllCopyResults();
      } catch (AmazonClientException e) {
        throw new CircusTrainException("Error in S3S3Copier:", e);
      }
    } finally {
      // cancel any running tasks
      if (transferManager != null) {
        transferManager.shutdownNow();
      }
    }
  }

  private void startAllCopyJobs() {
    AmazonS3URI sourceBase = toAmazonS3URI(sourceBaseLocation.toUri());
    AmazonS3URI targetBase = toAmazonS3URI(replicaLocation.toUri());
    srcClient = s3ClientFactory.newInstance(sourceBase, s3s3CopierOptions);
    targetClient = s3ClientFactory.newInstance(targetBase, s3s3CopierOptions);
    transferManager = transferManagerFactory.newInstance(targetClient, s3s3CopierOptions);
    if (sourceSubLocations.isEmpty()) {
      copy(sourceBase, targetBase);
    } else {
      for (Path path : sourceSubLocations) {
        AmazonS3URI subLocation = toAmazonS3URI(path.toUri());
        String partitionKey = StringUtils.removeStart(subLocation.getKey(), sourceBase.getKey());
        partitionKey = StringUtils.removeStart(partitionKey, "/");
        AmazonS3URI targetS3Uri = toAmazonS3URI(new Path(replicaLocation, partitionKey).toUri());
        LOG.debug("Starting copyJob from {} to {}", subLocation, targetS3Uri);
        copy(subLocation, targetS3Uri);
      }
    }
  }

  private void copy(AmazonS3URI source, AmazonS3URI target) {
    ListObjectsRequest request = listObjectsRequestFactory
        .newInstance()
        .withBucketName(source.getBucket())
        .withPrefix(source.getKey());
    ObjectListing listing = srcClient.listObjects(request);
    submitCopyJobsFromListing(source, target, request, listing);
    while (listing.isTruncated()) {
      listing = srcClient.listNextBatchOfObjects(listing);
      submitCopyJobsFromListing(source, target, request, listing);
    }
  }

  private void submitCopyJobsFromListing(
      AmazonS3URI sourceS3Uri,
      final AmazonS3URI targetS3Uri,
      ListObjectsRequest request,
      ObjectListing listing) {
    LOG
        .debug("Found objects to copy {}, for request {}/{}", listing.getObjectSummaries(), request.getBucketName(),
            request.getPrefix());
    List<S3ObjectSummary> objectSummaries = listing.getObjectSummaries();
    for (final S3ObjectSummary s3ObjectSummary : objectSummaries) {
      String fileName = StringUtils.removeStart(s3ObjectSummary.getKey(), sourceS3Uri.getKey());
      final String targetKey = Strings.nullToEmpty(targetS3Uri.getKey()) + fileName;
      LOG
          .info("copying object from '{}/{}' to '{}/{}'", s3ObjectSummary.getBucketName(), s3ObjectSummary.getKey(),
              targetS3Uri.getBucket(), targetKey);

      CopyObjectRequest copyObjectRequest = new CopyObjectRequest(s3ObjectSummary.getBucketName(),
          s3ObjectSummary.getKey(), targetS3Uri.getBucket(), targetKey);
      applyObjectMetadata(copyObjectRequest);

      TransferStateChangeListener stateChangeListener = new BytesTransferStateChangeListener(s3ObjectSummary,
          targetS3Uri, targetKey);
      Copy copy = transferManager.copy(copyObjectRequest, srcClient, stateChangeListener);
      totalBytesToReplicate += copy.getProgress().getTotalBytesToTransfer();
      copyJobs.add(copy);
    }
  }

  private void applyObjectMetadata(CopyObjectRequest copyObjectRequest) {
    if (s3s3CopierOptions.isS3ServerSideEncryption()) {
      ObjectMetadata objectMetadata = new ObjectMetadata();
      objectMetadata.setSSEAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);
      copyObjectRequest.setNewObjectMetadata(objectMetadata);
    }
  }

  private Metrics gatherAllCopyResults() {
    AtomicLong bytesReplicated = new AtomicLong(0);
    registerRunningMetrics(bytesReplicated);
    for (Copy copyJob : copyJobs) {
      try {
        copyJob.waitForCompletion();
        long alreadyReplicated = bytesReplicated.addAndGet(copyJob.getProgress().getTotalBytesToTransfer());
        if (totalBytesToReplicate > 0) {
          LOG
              .info("Replicating...': {}% complete",
                  String.format("%.0f", (alreadyReplicated / (double) totalBytesToReplicate) * 100.0));
        }
      } catch (InterruptedException e) {
        throw new CircusTrainException(e);
      }
    }
    ImmutableMap<String, Long> metrics = ImmutableMap
        .of(S3S3CopierMetrics.Metrics.TOTAL_BYTES_TO_REPLICATE.name(), totalBytesToReplicate);
    return new S3S3CopierMetrics(metrics, bytesReplicated.get());
  }

  private void registerRunningMetrics(final AtomicLong bytesReplicated) {
    Gauge<Long> gauge = new AtomicLongGauge(bytesReplicated);
    registry.remove(RunningMetrics.S3S3_CP_BYTES_REPLICATED.name());
    registry.register(RunningMetrics.S3S3_CP_BYTES_REPLICATED.name(), gauge);
  }
}
