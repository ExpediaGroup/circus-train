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

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.retry.RetryCallback;
import org.springframework.retry.RetryContext;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.transfer.Copy;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.internal.TransferStateChangeListener;

public class RetryableTransferManager {

  private static final Logger LOG = LoggerFactory.getLogger(RetryableTransferManager.class);
  private static final int INITIAL_RETRY_INTERVAL_MS = 500;
  private static final int MAX_RETRY_INTERVAL_MS = 1000;

  private RetryTemplate retryTemplate;
  private TransferManager transferManager;
  private AmazonS3 srcClient;
  private int maxAttempts;

  public RetryableTransferManager(TransferManager transferManager, AmazonS3 srcClient, int maxAttempts) {
    this.transferManager = transferManager;
    this.srcClient = srcClient;
    this.maxAttempts = maxAttempts;
    this.retryTemplate = setUpRetryTemplate();
  }

  public Copy copy(final CopyObjectRequest copyObjectRequest, final TransferStateChangeListener stateChangeListener) {
    try {
      return retryTemplate.execute(new RetryCallback<Copy, Throwable>() {
        public Copy doWithRetry(RetryContext context) {
          LOG.info("copying attempt {}/{}", context.getRetryCount()+1, maxAttempts);
          return transferManager.copy(copyObjectRequest, srcClient, stateChangeListener);
        }
      });
    } catch (Throwable throwable) {
      throw new AmazonS3Exception(throwable.getMessage());
    }
  }

  public void shutdownNow() {
    transferManager.shutdownNow();
  }

  private RetryTemplate setUpRetryTemplate() {
    Map<Class<? extends Throwable>, Boolean> exceptions = new HashMap<>();
    exceptions.put(AmazonS3Exception.class, true);

    SimpleRetryPolicy retryPolicy = new SimpleRetryPolicy(maxAttempts, exceptions);

    ExponentialBackOffPolicy backOffPolicy = new ExponentialBackOffPolicy();
    backOffPolicy.setInitialInterval(INITIAL_RETRY_INTERVAL_MS);
    backOffPolicy.setMaxInterval(MAX_RETRY_INTERVAL_MS);

    RetryTemplate retryTemplate = new RetryTemplate();
    retryTemplate.setRetryPolicy(retryPolicy);
    retryTemplate.setBackOffPolicy(backOffPolicy);

    return retryTemplate;
  }
}
