/**
 * Copyright (C) 2016-2021 Expedia, Inc.
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

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import org.junit.Test;
import org.mockito.Mock;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonWebServiceRequest;

public class ExponentialBackoffStrategyTest {

  private static final int ERROR_RETRY_DELAY = 10;

  private @Mock AmazonWebServiceRequest originalRequest;
  private @Mock AmazonClientException exception;

  private ExponentialBackoffStrategy strategy = new ExponentialBackoffStrategy(ERROR_RETRY_DELAY);

  @Test
  public void firstBackoff() {
    assertThat(strategy.delayBeforeNextRetry(originalRequest, exception, 1), is(1L * ERROR_RETRY_DELAY));
  }

  @Test
  public void secondBackoff() {
    assertThat(strategy.delayBeforeNextRetry(originalRequest, exception, 2), is(2L * ERROR_RETRY_DELAY));
  }

  @Test
  public void thirdBackoff() {
    assertThat(strategy.delayBeforeNextRetry(originalRequest, exception, 3), is(3L * ERROR_RETRY_DELAY));
  }

}
