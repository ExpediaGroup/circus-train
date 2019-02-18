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
package com.hotels.bdp.circustrain.s3s3copier.aws;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.transfer.Copy;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.internal.TransferStateChangeListener;

public class RetryableTransferManager {

  private static final Logger LOG = LoggerFactory.getLogger(RetryableTransferManager.class);
  private static final int BACKOFF_DELAY_MS = 500;
  private static final int MAX_ATTEMPTS = 3;
  private TransferManager transferManager;
  private AmazonS3 srcClient;
  private int retryAttempt = 0;

  public RetryableTransferManager(TransferManager transferManager, AmazonS3 srcClient) {
    this.transferManager = transferManager;
    this.srcClient = srcClient;
  }

  @Retryable(value = { AmazonS3Exception.class },
      maxAttempts = MAX_ATTEMPTS,
      backoff = @Backoff(delay = BACKOFF_DELAY_MS, multiplier = 2))
  public Copy copy(CopyObjectRequest copyObjectRequest, TransferStateChangeListener stateChangeListener) {
    retryAttempt++;
    LOG.info("copying attempt {}/3", retryAttempt);
    Copy copy = transferManager.copy(copyObjectRequest, srcClient, stateChangeListener);
    retryAttempt = 0;
    return copy;
  }

  public void shutdownNow() {
    transferManager.shutdownNow();
  }
}
