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
package com.hotels.bdp.circustrain.aws;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;


public class S3SchemesTest {

  @Test
  public void isS3Scheme() {
    assertTrue(S3Schemes.isS3Scheme("s3"));
    assertTrue(S3Schemes.isS3Scheme("S3"));
    assertTrue(S3Schemes.isS3Scheme("s3a"));
    assertTrue(S3Schemes.isS3Scheme("S3A"));
    assertTrue(S3Schemes.isS3Scheme("s3n"));
    assertTrue(S3Schemes.isS3Scheme("S3N"));
  }

  @Test
  public void isNotS3Scheme() {
    assertFalse(S3Schemes.isS3Scheme(null));
    assertFalse(S3Schemes.isS3Scheme(""));
    assertFalse(S3Schemes.isS3Scheme("file"));
    assertFalse(S3Schemes.isS3Scheme("s321"));
    assertFalse(S3Schemes.isS3Scheme("sss"));
  }
}
