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

import static org.hamcrest.CoreMatchers.instanceOf;
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
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.transfer.Copy;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.internal.TransferStateChangeListener;

@RunWith(MockitoJUnitRunner.class)
public class RetryableTransferManagerTest {

  private static final int MAX_COPY_ATTEMPTS = 3;

  private @Mock TransferManager mockedTransferManager;
  private RetryableTransferManager retryableTransferManager;

  @Before
  public void setup() {
    Mockito.reset(mockedTransferManager);
    retryableTransferManager = new RetryableTransferManager(mockedTransferManager, Mockito.mock(AmazonS3.class), MAX_COPY_ATTEMPTS);
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
      assertThat(e.getCause(), instanceOf(AmazonClientException.class));
      assertThat(e.getCause().getMessage(), startsWith("S3 error"));
    }
  }
}
