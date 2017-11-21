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
package com.hotels.bdp.circustrain.aws;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;

public class AWSCredentialUtilsTest {

  public @Rule RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  @Test
  public void getAccessKeyFromConfTest() throws Exception {
    Configuration conf = new Configuration();
    conf.set(AWSConstants.ACCESS_KEY, "access");
    conf.set(AWSConstants.SECRET_KEY, "secret");
    String access = AWSCredentialUtils.getKey(conf, AWSConstants.ACCESS_KEY);
    assertThat(access, is("access"));
  }

  @Test
  public void getSecretKeyFromConfTest() throws Exception {
    Configuration conf = new Configuration();
    conf.set(AWSConstants.ACCESS_KEY, "access");
    conf.set(AWSConstants.SECRET_KEY, "secret");
    String secret = AWSCredentialUtils.getKey(conf, AWSConstants.SECRET_KEY);
    assertThat(secret, is("secret"));
  }

  @Test(expected = IllegalStateException.class)
  public void getKeyFromConfWhichIsntSetThrowsExceptionTest() throws Exception {
    Configuration conf = new Configuration();
    AWSCredentialUtils.getKey(conf, AWSConstants.SECRET_KEY);
  }

  @Test(expected = RuntimeException.class)
  public void getKeyThrowsRuntimeExceptionTest() throws Exception {
    Configuration conf = mock(Configuration.class);
    when(conf.getPassword(anyString())).thenThrow(new IOException());
    AWSCredentialUtils.getKey(conf, AWSConstants.SECRET_KEY);
  }

}
