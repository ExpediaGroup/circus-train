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
package com.hotels.bdp.circustrain.s3s3copier.aws;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3URI;

import com.hotels.bdp.circustrain.s3s3copier.S3S3CopierOptions;

public interface AmazonS3ClientFactory {

  /**
   * @param uri Used for region discovery. The returned client will be in the same region as the bucket described in the
   *          URI.
   * @param s3s3CopierOptions Copier options.
   * @return a new instance of {@linkplain AmazonS3 AmazonS3}
   */
  AmazonS3 newInstance(AmazonS3URI uri, S3S3CopierOptions s3s3CopierOptions);

}
