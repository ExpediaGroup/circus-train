/**
 * Copyright (C) 2016-2017 Expedia Inc.
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
package com.hotels.bdp.circustrain.s3mapreducecp.aws;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.retry.RetryPolicy.RetryCondition;

class CounterBasedRetryCondition implements RetryCondition {
  private static final Logger LOG = LoggerFactory.getLogger(CounterBasedRetryCondition.class);

  private final int maxErrorRetry;

  CounterBasedRetryCondition(int maxErrorRetry) {
    this.maxErrorRetry = maxErrorRetry;
  }

  @Override
  public boolean shouldRetry(
      AmazonWebServiceRequest originalRequest,
      AmazonClientException exception,
      int retriesAttempted) {
    LOG.debug("Exception caught during upload, retries attempted = {} out of {}", retriesAttempted, maxErrorRetry,
        exception);
    return retriesAttempted <= maxErrorRetry;
  }
}