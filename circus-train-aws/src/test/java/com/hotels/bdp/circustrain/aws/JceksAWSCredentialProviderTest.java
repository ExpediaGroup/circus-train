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
package com.hotels.bdp.circustrain.aws;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.when;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import fm.last.commons.test.file.ClassDataFolder;

import com.amazonaws.auth.AWSCredentials;

@RunWith(MockitoJUnitRunner.class)
public class JceksAWSCredentialProviderTest {

  public @Rule ClassDataFolder dataFolder = new ClassDataFolder();

  private @Mock Configuration conf;

  @Test
  public void credentialsFromFile() throws IOException {
    String jceksPath = "jceks://file" + dataFolder.getFile("aws.jceks").getAbsolutePath();
    JceksAWSCredentialProvider provider = new JceksAWSCredentialProvider(jceksPath);
    AWSCredentials credentials = provider.getCredentials();
    assertThat(credentials.getAWSAccessKeyId(), is("access"));
    assertThat(credentials.getAWSSecretKey(), is("secret"));
  }

  @Test
  public void credentialsFromConf() throws IOException {
    when(conf.getPassword(AWSConstants.ACCESS_KEY)).thenReturn("accessKey".toCharArray());
    when(conf.getPassword(AWSConstants.SECRET_KEY)).thenReturn("secretKey".toCharArray());
    JceksAWSCredentialProvider provider = new JceksAWSCredentialProvider(conf);
    AWSCredentials credentials = provider.getCredentials();
    assertThat(credentials.getAWSAccessKeyId(), is("accessKey"));
    assertThat(credentials.getAWSSecretKey(), is("secretKey"));
  }

  @Test(expected = NullPointerException.class)
  public void nullJceksPath() {
    new JceksAWSCredentialProvider((String) null);
  }

  @Test(expected = NullPointerException.class)
  public void nullConfiguration() {
    new JceksAWSCredentialProvider((Configuration) null);
  }

  @Test(expected = IllegalStateException.class)
  public void secretKeyNotSetInConfThrowsException() throws Exception {
    when(conf.getPassword(AWSConstants.ACCESS_KEY)).thenReturn("accessKey".toCharArray());
    new JceksAWSCredentialProvider(conf).getCredentials();
  }

  @Test(expected = IllegalStateException.class)
  public void accessKeyNotSetInConfThrowsException() throws Exception {
    new JceksAWSCredentialProvider(conf).getCredentials();
  }

}
