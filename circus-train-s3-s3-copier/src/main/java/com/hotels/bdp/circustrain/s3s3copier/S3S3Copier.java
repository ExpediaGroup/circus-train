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

  private static final Logger LOG = LoggerFactory.getLogger(S3S3Copier.class);

  private static class BytesTransferStateChangeListener implements TransferStateChangeListener {

    private final S3ObjectSummary s3ObjectSummary;
    private final AmazonS3URI targetS3Uri;
    private final String targetKey;

    private BytesTransferStateChangeListener(
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
  private final List<CopyJobRequest> copyJobRequests = new ArrayList<>();

  private long totalBytesToReplicate = 0;
  private AtomicLong bytesReplicated = new AtomicLong(0);
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
    registerRunningMetrics(bytesReplicated);
    try {
      try {
        initialiseAllCopyRequests();
        processAllCopyJobs();
        return gatherMetrics();
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

  private void initialiseAllCopyRequests() {
    LOG
        .info("Initialising all copy jobs");
    AmazonS3URI sourceBase = toAmazonS3URI(sourceBaseLocation.toUri());
    AmazonS3URI targetBase = toAmazonS3URI(replicaLocation.toUri());
    srcClient = s3ClientFactory.newInstance(sourceBase, s3s3CopierOptions);
    targetClient = s3ClientFactory.newInstance(targetBase, s3s3CopierOptions);


    transferManager = transferManagerFactory.newInstance(targetClient, s3s3CopierOptions);
    if (sourceSubLocations.isEmpty()) {
      initialiseCopyJobs(sourceBase, targetBase);
    } else {
      for (Path path : sourceSubLocations) {
        AmazonS3URI subLocation = toAmazonS3URI(path.toUri());
        String partitionKey = StringUtils.removeStart(subLocation.getKey(), sourceBase.getKey());
        partitionKey = StringUtils.removeStart(partitionKey, "/");
        AmazonS3URI targetS3Uri = toAmazonS3URI(new Path(replicaLocation, partitionKey).toUri());
        initialiseCopyJobs(subLocation, targetS3Uri);
      }
    }
    LOG
        .info("Finished initialising {} copy job(s)", copyJobRequests.size());
  }

  private void initialiseCopyJobs(AmazonS3URI source, AmazonS3URI target) {
    ListObjectsRequest request = listObjectsRequestFactory
        .newInstance()
        .withBucketName(source.getBucket())
        .withPrefix(source.getKey());
    ObjectListing listing = srcClient.listObjects(request);
    initialiseCopyJobsFromListing(source, target, request, listing);
    while (listing.isTruncated()) {
      listing = srcClient.listNextBatchOfObjects(listing);
      initialiseCopyJobsFromListing(source, target, request, listing);
    }
  }

  private void initialiseCopyJobsFromListing(
      AmazonS3URI sourceS3Uri,
      final AmazonS3URI targetS3Uri,
      ListObjectsRequest request,
      ObjectListing listing) {
    LOG
        .debug("Found objects to copy {}, for request {}/{}", listing.getObjectSummaries(), request.getBucketName(),
            request.getPrefix());
    List<S3ObjectSummary> objectSummaries = listing.getObjectSummaries();
    for (final S3ObjectSummary s3ObjectSummary : objectSummaries) {
      totalBytesToReplicate += s3ObjectSummary.getSize();
      String fileName = StringUtils.removeStart(s3ObjectSummary.getKey(), sourceS3Uri.getKey());
      final String targetKey = Strings.nullToEmpty(targetS3Uri.getKey()) + fileName;
      CopyObjectRequest copyObjectRequest = new CopyObjectRequest(s3ObjectSummary.getBucketName(),
          s3ObjectSummary.getKey(), targetS3Uri.getBucket(), targetKey);

      if (s3s3CopierOptions.getCannedAcl() != null) {
        copyObjectRequest.withCannedAccessControlList(s3s3CopierOptions.getCannedAcl());
      }

      applyObjectMetadata(copyObjectRequest);

      TransferStateChangeListener stateChangeListener = new BytesTransferStateChangeListener(s3ObjectSummary,
          targetS3Uri, targetKey);
      copyJobRequests.add(new CopyJobRequest(copyObjectRequest, stateChangeListener));
    }
  }

  private void applyObjectMetadata(CopyObjectRequest copyObjectRequest) {
    if (s3s3CopierOptions.isS3ServerSideEncryption()) {
      ObjectMetadata objectMetadata = new ObjectMetadata();
      objectMetadata.setSSEAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);
      copyObjectRequest.setNewObjectMetadata(objectMetadata);
    }
  }

  private void processAllCopyJobs() {
    List<CopyJobRequest> copyJobsToSubmit = copyJobRequests;
    int maxCopyAttempts = s3s3CopierOptions.getMaxCopyAttempts();
    for (int copyAttempt = 1; copyAttempt <= maxCopyAttempts; copyAttempt++) {
      LOG
          .info("Submitting {} copy job(s), attempt {}/{}", copyJobsToSubmit.size(), copyAttempt, maxCopyAttempts);
      copyJobsToSubmit = submitAndGatherCopyJobs(copyJobsToSubmit);
      if (copyJobsToSubmit.isEmpty()) {
        LOG
            .info("Successfully gathered all copy jobs on attempt {}/{}", copyAttempt, maxCopyAttempts);
        return;
      }
      if (copyAttempt == maxCopyAttempts) {
        throw new CircusTrainException(copyJobsToSubmit.size() + " job(s) failed the maximum number of copy attempts, " + maxCopyAttempts);
      }
      LOG
          .info("Finished gathering jobs on attempt {}/{}. Retrying {} failed job(s).",
              copyAttempt,
              maxCopyAttempts,
              copyJobsToSubmit.size());
    }
  }

  private List<CopyJobRequest> submitAndGatherCopyJobs(List<CopyJobRequest> copyJobsToSubmit) {
    List<CopyJob> submittedCopyJobs = new ArrayList<>();
    for (CopyJobRequest copyJobRequest : copyJobsToSubmit) {
      Copy copy = submitCopyJob(copyJobRequest);
      CopyJob newCopyJob = new CopyJob(copy, copyJobRequest);
      submittedCopyJobs.add(newCopyJob);
    }
    return gatherCopyJobs(submittedCopyJobs);
  }

  private Copy submitCopyJob(CopyJobRequest copyJob) {
    CopyObjectRequest copyObjectRequest = copyJob.getCopyObjectRequest();
    LOG
        .info("Copying object from '{}/{}' to '{}/{}'", copyObjectRequest.getSourceBucketName(), copyObjectRequest.getSourceKey(),
            copyObjectRequest.getDestinationBucketName(), copyObjectRequest.getDestinationKey());
    return transferManager.copy(copyObjectRequest, srcClient, copyJob.getTransferStateChangeListener());
  }

  /**
   * Waits for all running copy jobs to complete and updates overall progress.
   * @param copyJobs A list of copy jobs which have been submitted
   * @return A list of failed copy job requests
   */
  private List<CopyJobRequest> gatherCopyJobs(List<CopyJob> copyJobs) {
    List<CopyJobRequest> failedCopyJobRequests = new ArrayList<>();
    for (CopyJob copyJob : copyJobs) {
      try {
        Copy copy = copyJob.getCopy();
        try {
          copy.waitForCompletion();
          long alreadyReplicated = bytesReplicated.addAndGet(copy.getProgress().getTotalBytesToTransfer());
          if (totalBytesToReplicate > 0) {
            LOG
                .info("Replicating...': {}% complete",
                    String.format("%.0f", (alreadyReplicated / (double) totalBytesToReplicate) * 100.0));
          }
        } catch (AmazonClientException e) {
          CopyObjectRequest copyObjectRequest = copyJob.getCopyJobRequest().getCopyObjectRequest();
          LOG
              .info("Copying '{}/{}' failed, adding to retry list.",
                  copyObjectRequest.getSourceBucketName(),
                  copyObjectRequest.getSourceKey());
          LOG
              .error("Copy failed with exception:", e);
          failedCopyJobRequests.add(copyJob.getCopyJobRequest());
        }
      } catch (InterruptedException e) {
        throw new CircusTrainException(e);
      }
    }
    return failedCopyJobRequests;
  }

  private Metrics gatherMetrics() {
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
