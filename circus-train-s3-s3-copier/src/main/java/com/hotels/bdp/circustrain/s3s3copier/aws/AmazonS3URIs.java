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
package com.hotels.bdp.circustrain.s3s3copier.aws;

import static com.hotels.bdp.circustrain.aws.AWSConstants.FS_PROTOCOL_S3;

import java.net.URI;
import java.net.URISyntaxException;

import com.amazonaws.services.s3.AmazonS3URI;

import com.hotels.bdp.circustrain.aws.S3Schemes;

public final class AmazonS3URIs {

  private AmazonS3URIs() {}

  public static AmazonS3URI toAmazonS3URI(URI uri) {
    if (FS_PROTOCOL_S3.equalsIgnoreCase(uri.getScheme())) {
      return new AmazonS3URI(uri);
    } else if (S3Schemes.isS3Scheme(uri.getScheme())) {
      try {
        return new AmazonS3URI(new URI(FS_PROTOCOL_S3, uri.getUserInfo(), uri.getHost(), uri.getPort(), uri.getPath(),
            uri.getQuery(), uri.getFragment()));
      } catch (URISyntaxException e) {
        // ignore it, it will fail on the return
      }
    }
    // Build it anyway we'll get AmazonS3URI exception back.
    return new AmazonS3URI(uri);
  }
}
