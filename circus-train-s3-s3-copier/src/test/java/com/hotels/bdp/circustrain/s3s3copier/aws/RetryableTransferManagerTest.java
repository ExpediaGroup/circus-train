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

import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.EnableAspectJAutoProxy;
import org.springframework.retry.annotation.EnableRetry;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.transfer.Copy;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.internal.TransferStateChangeListener;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public class RetryableTransferManagerTest {

  @Autowired
  private TransferManager mockedTransferManager;

  @Autowired
  private RetryableTransferManager retryableTransferManager;

  @Before
  public void setup() {
    Mockito.reset(mockedTransferManager);
  }

  @Test
  public void copyRetryWhenS3ThrowsException() throws Exception {
    CopyObjectRequest copyObjectRequest = Mockito.mock(CopyObjectRequest.class);
    TransferStateChangeListener stateChangeListener = Mockito.mock(TransferStateChangeListener.class);
    Copy copy = Mockito.mock(Copy.class);
    when(mockedTransferManager.copy(any(CopyObjectRequest.class), any(AmazonS3.class),
        any(TransferStateChangeListener.class))).thenThrow(new AmazonS3Exception("S3 error"))
        .thenThrow(new AmazonS3Exception("S3 error"))
        .thenReturn(copy);
    try {
      Copy copyResult = retryableTransferManager.copy(copyObjectRequest, stateChangeListener);
      assertThat(copyResult, is(copy));
    } catch (Exception e) {
      fail("Should not have thrown exception");
    }
    verify(mockedTransferManager, times(3)).copy(any(CopyObjectRequest.class),
        any(AmazonS3.class), any(TransferStateChangeListener.class));
  }

  @Test
  public void copyThrowExceptionWhenS3ThrowsExceptionThreeTimes() throws Exception {
    CopyObjectRequest copyObjectRequest = Mockito.mock(CopyObjectRequest.class);
    TransferStateChangeListener stateChangeListener = Mockito.mock(TransferStateChangeListener.class);
    when(mockedTransferManager.copy(any(CopyObjectRequest.class), any(AmazonS3.class),
        any(TransferStateChangeListener.class))).thenThrow(new AmazonS3Exception("S3 error"))
        .thenThrow(new AmazonS3Exception("S3 error"))
        .thenThrow(new AmazonS3Exception("S3 error"));
    try {
      retryableTransferManager.copy(copyObjectRequest, stateChangeListener);
      fail("Exception should have been thrown");
    } catch (Exception e) {
      verify(mockedTransferManager, times(3)).copy(any(CopyObjectRequest.class),
          any(AmazonS3.class), any(TransferStateChangeListener.class));
      assertThat(e.getMessage(), startsWith("S3 error"));
    }
  }

  @Configuration
  @EnableRetry
  @EnableAspectJAutoProxy(proxyTargetClass=true)
  public static class ContextConfiguration {
    @Bean
    public TransferManager transferManager() {
      return Mockito.mock(TransferManager.class);
    }

    @Bean
    public RetryableTransferManager retriableTransferManager(TransferManager transferManager) {
      AmazonS3 srcClient = Mockito.mock(AmazonS3.class);
      return new RetryableTransferManager(transferManager, srcClient);
    }
  }
}
