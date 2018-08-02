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
package com.hotels.bdp.circustrain.gcp.context;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.hotels.bdp.circustrain.context.CommonBeans;
import com.hotels.bdp.circustrain.gcp.BindGoogleHadoopFileSystem;
import com.hotels.bdp.circustrain.gcp.DistributedFileSystemPathProvider;
import com.hotels.bdp.circustrain.gcp.FileSystemFactory;
import com.hotels.bdp.circustrain.gcp.GCPCredentialCopier;
import com.hotels.bdp.circustrain.gcp.GCPCredentialPathProvider;
import com.hotels.bdp.circustrain.gcp.RandomStringFactory;

@RunWith(MockitoJUnitRunner.class)
public class GCPBeanPostProcessorTest {
  private @Mock RandomStringFactory randomStringFactory;

  private @Mock GCPSecurity security;
  private @Mock GCPCredentialPathProvider credentialPathProvider;
  private @Mock DistributedFileSystemPathProvider distributedFileSystemPathProvider;
  private @Mock Configuration configuration;
  private @Mock BindGoogleHadoopFileSystem bindGoogleHadoopFileSystem;
  private @Mock FileSystemFactory fileSystemFactory;
  private @Mock GCPCredentialCopier credentialCopier;

  private GCPBeanPostProcessor processor;

  @Before
  public void init() {
    doNothing().when(configuration).set(anyString(), anyString());
    processor = new GCPBeanPostProcessor(credentialPathProvider, distributedFileSystemPathProvider,
        bindGoogleHadoopFileSystem, fileSystemFactory, credentialCopier);
  }

  @Test
  public void postProcessAfterInitializationWithCorrectBeanName() throws Exception {
    String beanName = CommonBeans.BEAN_BASE_CONF;
    processor.postProcessAfterInitialization(configuration, beanName);
    verify(credentialPathProvider).newPath();
  }

  @Test
  public void postProcessAfterInitializationWithIncorrectBeanName() throws Exception {
    String beanName = "notBaseConf";
    processor.postProcessAfterInitialization(configuration, beanName);
    verify(credentialPathProvider, times(0)).newPath();
  }

  @Test
  public void postProcessAfterInitializationWithBlankCredentialProviderDoesntModifyConfiguration() throws Exception {
    String beanName = CommonBeans.BEAN_BASE_CONF;
    processor.postProcessAfterInitialization(configuration, beanName);
    when(security.getCredentialProvider()).thenReturn("");
    verify(credentialPathProvider).newPath();
    verify(configuration, times(0)).set(anyString(), anyString());
  }

  @Test
  public void postProcessorWithNonNullCredentialProvider() {
    String beanName = CommonBeans.BEAN_BASE_CONF;
    doReturn(new Path("/test.json")).when(credentialPathProvider).newPath();
    processor.postProcessAfterInitialization(configuration, beanName);
    verify(credentialPathProvider).newPath();
    verify(bindGoogleHadoopFileSystem).bindFileSystem(configuration);
  }
}
