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
package com.hotels.bdp.circustrain.aws.sns.event;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.sns.AmazonSNS;

@RunWith(MockitoJUnitRunner.class)
public class SnsConfigurationTest {

  private @Mock Configuration conf;

  private final SnsConfiguration configuration = new SnsConfiguration();

  @Test
  public void credentials() throws IOException {
    when(conf.getPassword("access.key")).thenReturn("accessKey".toCharArray());
    when(conf.getPassword("secret.key")).thenReturn("secretKey".toCharArray());
    AWSCredentialsProvider credentialsProvider = configuration.awsCredentialsProvider(conf);
    AWSCredentials awsCredentials = credentialsProvider.getCredentials();
    assertThat(awsCredentials.getAWSAccessKeyId(), is("accessKey"));
    assertThat(awsCredentials.getAWSSecretKey(), is("secretKey"));
  }

  @Test
  public void snsClient() {
    AWSCredentialsProvider credentialsProvider = mock(AWSCredentialsProvider.class);
    when(credentialsProvider.getCredentials()).thenReturn(new BasicAWSCredentials("accessKey", "secretKey"));
    ListenerConfig config = new ListenerConfig();
    config.setRegion("eu-west-1");
    AmazonSNS sns = configuration.amazonSNS(config, credentialsProvider);
    assertThat(sns, is(not(nullValue())));
  }

}
