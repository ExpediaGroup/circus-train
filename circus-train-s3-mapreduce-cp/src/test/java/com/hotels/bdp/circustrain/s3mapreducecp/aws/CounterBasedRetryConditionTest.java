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
package com.hotels.bdp.circustrain.s3mapreducecp.aws;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonWebServiceRequest;

@RunWith(MockitoJUnitRunner.class)
public class CounterBasedRetryConditionTest {

  private static final int MAX_ERROR_RETRY = 2;

  private @Mock AmazonWebServiceRequest originalRequest;
  private @Mock AmazonClientException exception;

  private CounterBasedRetryCondition condition = new CounterBasedRetryCondition(MAX_ERROR_RETRY);

  @Test
  public void doRetry() {
    assertThat(condition.shouldRetry(originalRequest, exception, MAX_ERROR_RETRY - 1), is(true));
  }

  @Test
  public void doRetryWhenLimitIsReached() {
    assertThat(condition.shouldRetry(originalRequest, exception, MAX_ERROR_RETRY), is(true));
  }

  @Test
  public void doNotRetry() {
    assertThat(condition.shouldRetry(originalRequest, exception, MAX_ERROR_RETRY + 1), is(false));
  }

}
