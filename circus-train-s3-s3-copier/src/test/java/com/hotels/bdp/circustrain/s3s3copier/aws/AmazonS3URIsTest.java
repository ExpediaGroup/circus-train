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

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.net.URI;

import org.junit.Test;

import com.amazonaws.services.s3.AmazonS3URI;

public class AmazonS3URIsTest {

  @Test
  public void toAmazonS3URISchemeIsS3() throws Exception {
    AmazonS3URI result = AmazonS3URIs.toAmazonS3URI(new URI("s3://a/b"));
    AmazonS3URI expected = new AmazonS3URI("s3://a/b");
    assertThat(result, is(expected));
  }

  @Test
  public void toAmazonS3URISchemeIsS3Uppercase() throws Exception {
    AmazonS3URI result = AmazonS3URIs.toAmazonS3URI(new URI("S3://a/b"));
    AmazonS3URI expected = new AmazonS3URI("s3://a/b");
    assertThat(result, is(expected));
  }

  @Test
  public void toAmazonS3URISchemeIsS3a() throws Exception {
    AmazonS3URI result = AmazonS3URIs.toAmazonS3URI(new URI("s3a://a/b"));
    AmazonS3URI expected = new AmazonS3URI("s3://a/b");
    assertThat(result, is(expected));
  }

  @Test
  public void toAmazonS3URISchemeIsS3aUppercase() throws Exception {
    AmazonS3URI result = AmazonS3URIs.toAmazonS3URI(new URI("S3A://a/b"));
    AmazonS3URI expected = new AmazonS3URI("s3://a/b");
    assertThat(result, is(expected));
  }

  @Test
  public void toAmazonS3URISchemeIsS3n() throws Exception {
    AmazonS3URI result = AmazonS3URIs.toAmazonS3URI(new URI("s3n://a/b"));
    AmazonS3URI expected = new AmazonS3URI("s3://a/b");
    assertThat(result, is(expected));
  }

  @Test
  public void toAmazonS3URISchemeIsS3nUppercase() throws Exception {
    AmazonS3URI result = AmazonS3URIs.toAmazonS3URI(new URI("S3N://a/b"));
    AmazonS3URI expected = new AmazonS3URI("s3://a/b");
    assertThat(result, is(expected));
  }

  @Test(expected = IllegalArgumentException.class)
  public void toAmazonS3URISchemeIsInvalidS3Scheme() throws Exception {
    AmazonS3URIs.toAmazonS3URI(new URI("s321://a/b"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void toAmazonS3URISchemeIsNonAmazon() throws Exception {
    AmazonS3URIs.toAmazonS3URI(new URI("file://a/b"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void toAmazonS3URISchemeIsNullScheme() throws Exception {
    AmazonS3URIs.toAmazonS3URI(new URI("/a/b"));
  }

}
