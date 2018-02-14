/**
 * Copyright (C) 2016-2018 Expedia Inc.
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
package com.hotels.bdp.circustrain.s3s3copier;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

public class S3S3CopierOptionsTest {

  private final Map<String, Object> copierOptions = new HashMap<>();

  @Test
  public void getMultipartCopyThreshold() throws Exception {
    copierOptions.put(S3S3CopierOptions.Keys.MULTIPART_COPY_THRESHOLD.keyName(), 128L);
    S3S3CopierOptions options = new S3S3CopierOptions(copierOptions);
    assertThat(options.getMultipartCopyThreshold(), is(128L));
  }

  @Test
  public void getMultipartCopyThresholdDefaultIsNull() throws Exception {
    S3S3CopierOptions options = new S3S3CopierOptions(copierOptions);
    assertNull(options.getMultipartCopyThreshold());
  }

  @Test
  public void getMultipartCopyPartSize() throws Exception {
    copierOptions.put(S3S3CopierOptions.Keys.MULTIPART_COPY_PART_SIZE.keyName(), 128L);
    S3S3CopierOptions options = new S3S3CopierOptions(copierOptions);
    assertThat(options.getMultipartCopyPartSize(), is(128L));
  }

  @Test
  public void getMultipartCopyPartSizeDefaultIsNull() throws Exception {
    S3S3CopierOptions options = new S3S3CopierOptions(copierOptions);
    assertNull(options.getMultipartCopyPartSize());
  }

  @Test
  public void getS3Endpoint() throws Exception {
    copierOptions.put(S3S3CopierOptions.Keys.S3_ENDPOINT_URI.keyName(), "http://s3.endpoint/");
    S3S3CopierOptions options = new S3S3CopierOptions(copierOptions);
    assertThat(options.getS3Endpoint(), is(URI.create("http://s3.endpoint/")));
  }

  @Test
  public void getS3EndpointDefaultIsNull() throws Exception {
    S3S3CopierOptions options = new S3S3CopierOptions(copierOptions);
    assertNull(options.getS3Endpoint());
  }

  @Test
  public void getSSEAlgorithm() throws Exception {
    copierOptions.put(S3S3CopierOptions.Keys.SSE_ALGORITHM.keyName(), "AES256");
    S3S3CopierOptions options = new S3S3CopierOptions(copierOptions);
    assertThat(options.getSSEAlgorithm(), is("AES256"));
  }

  @Test
  public void getSSEAlgorithmDefaultIsNull() throws Exception {
    S3S3CopierOptions options = new S3S3CopierOptions(copierOptions);
    assertNull(options.getSSEAlgorithm());
  }

}
