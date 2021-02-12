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
package com.hotels.bdp.circustrain.s3s3copier.aws;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.HashMap;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerConfiguration;

import com.hotels.bdp.circustrain.s3s3copier.S3S3CopierOptions;

@RunWith(MockitoJUnitRunner.class)
public class TransferManagerFactoryTest {

  @Mock private AmazonS3 mockClient;
  private final Long MULTIPART_COPY_THRESHOLD_VALUE = 1L;
  private final Long MULTIPART_COPY_PART_SIZE = 1L;

  @Test
  public void shouldCreateDefaultTransferManagerClient() {
    S3S3CopierOptions s3Options = new S3S3CopierOptions(new HashMap<String, Object>() {{
      put(S3S3CopierOptions.Keys.MULTIPART_COPY_THRESHOLD.keyName(), MULTIPART_COPY_THRESHOLD_VALUE);
      put(S3S3CopierOptions.Keys.MULTIPART_COPY_PART_SIZE.keyName(), MULTIPART_COPY_PART_SIZE);
    }});

    TransferManagerFactory factory = new TransferManagerFactory();
    TransferManager transferManager = factory.newInstance(mockClient, s3Options);
    assertThat(transferManager.getAmazonS3Client(), is(mockClient));

    TransferManagerConfiguration managerConfig = transferManager.getConfiguration();
    assertThat(managerConfig.getMultipartCopyPartSize(), is(MULTIPART_COPY_PART_SIZE));
    assertThat(managerConfig.getMultipartCopyThreshold(), is(MULTIPART_COPY_THRESHOLD_VALUE));
  }
}
