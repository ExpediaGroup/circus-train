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
package com.hotels.bdp.circustrain.s3s3copier.aws;


import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;

import com.hotels.bdp.circustrain.s3s3copier.S3S3CopierOptions;

@Component
public class TransferManagerFactory {

  private static final Logger LOG = LoggerFactory.getLogger(TransferManagerFactory.class);

  public TransferManager newInstance(AmazonS3 targetS3Client, S3S3CopierOptions s3s3CopierOptions) {
    TransferManagerBuilder builder = TransferManagerBuilder.standard()
        .withMultipartCopyThreshold(s3s3CopierOptions.getMultipartCopyThreshold())
        .withMultipartCopyPartSize(s3s3CopierOptions.getMultipartCopyPartSize());


    if (s3s3CopierOptions.getThreadPoolThreadCount() != -1) {
      LOG.info("Initializing thread pool with {} threads", s3s3CopierOptions.getThreadPoolThreadCount());
      builder.withExecutorFactory(() -> Executors.newFixedThreadPool(s3s3CopierOptions.getThreadPoolThreadCount()));
    } else {
      builder.withS3Client(targetS3Client);
    }

    return builder.build();
  }
}
