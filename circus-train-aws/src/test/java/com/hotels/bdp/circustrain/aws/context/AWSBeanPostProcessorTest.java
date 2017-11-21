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
package com.hotels.bdp.circustrain.aws.context;

import static org.apache.hadoop.security.alias.CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.hotels.bdp.circustrain.aws.BindS3AFileSystem;
import com.hotels.bdp.circustrain.aws.S3CredentialsUtils;

@RunWith(MockitoJUnitRunner.class)
public class AWSBeanPostProcessorTest {

  @Mock
  private BindS3AFileSystem bindS3AFileSystem;
  @Mock
  private S3CredentialsUtils s3CredentialsUtils;
  private AWSBeanPostProcessor postProcessor;

  @Before
  public void setUp() {
    postProcessor = new AWSBeanPostProcessor(bindS3AFileSystem, s3CredentialsUtils);
  }

  @Test
  public void baseConfBindFileSystem() throws Exception {
    Configuration conf = new Configuration();
    Object result = postProcessor.postProcessAfterInitialization(conf, "baseConf");
    Configuration resultConf = (Configuration) result;
    assertThat(resultConf, is(conf));
    verify(bindS3AFileSystem).bindFileSystem(conf);
    verifyZeroInteractions(s3CredentialsUtils);
  }

  @Test
  public void baseConfs3Credentials() throws Exception {
    Configuration conf = new Configuration();
    conf.set(CREDENTIAL_PROVIDER_PATH, "path");
    Object result = postProcessor.postProcessAfterInitialization(conf, "baseConf");
    Configuration resultConf = (Configuration) result;
    assertThat(resultConf, is(conf));
    verify(bindS3AFileSystem).bindFileSystem(conf);
    verify(s3CredentialsUtils).setS3Credentials(conf);
  }

  @Test
  public void postProcessBeforeInitializationReturnsBean() throws Exception {
    Object bean = new Object();
    Object result = postProcessor.postProcessBeforeInitialization(bean, "bean");
    assertThat(result, is(bean));
  }

  @Test
  public void postProcessAfterInitializationReturnsBean() throws Exception {
    Object bean = new Object();
    Object result = postProcessor.postProcessAfterInitialization(bean, "bean");
    assertThat(result, is(bean));
  }

}
