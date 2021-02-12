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
package com.hotels.bdp.circustrain.gcp.context;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.hotels.bdp.circustrain.context.CommonBeans;
import com.hotels.bdp.circustrain.gcp.BindGoogleHadoopFileSystem;
import com.hotels.bdp.circustrain.gcp.DistributedFileSystemPathProvider;
import com.hotels.bdp.circustrain.gcp.FileSystemFactory;
import com.hotels.bdp.circustrain.gcp.GCPCredentialCopier;
import com.hotels.bdp.circustrain.gcp.GCPCredentialPathProvider;

@RunWith(MockitoJUnitRunner.class)
public class GCPBeanPostProcessorTest {

  private @Mock GCPCredentialPathProvider credentialPathProvider;
  private @Mock DistributedFileSystemPathProvider distributedFileSystemPathProvider;
  private @Mock Configuration configuration;
  private @Mock BindGoogleHadoopFileSystem bindGoogleHadoopFileSystem;
  private @Mock FileSystemFactory fileSystemFactory;
  private @Mock GCPCredentialCopier credentialCopier;
  private @Mock FileSystem fileSystem;

  private GCPBeanPostProcessor processor;

  @Before
  public void init() {
    processor = new GCPBeanPostProcessor(credentialPathProvider, distributedFileSystemPathProvider,
        bindGoogleHadoopFileSystem, fileSystemFactory, credentialCopier);
  }

  @Test
  public void postProcessAfterInitializationWithIncorrectBeanName() throws Exception {
    String beanName = "notBaseConf";
    processor.postProcessAfterInitialization(configuration, beanName);
    verifyZeroInteractions(credentialPathProvider, bindGoogleHadoopFileSystem, credentialCopier);
  }

  @Test
  public void postProcessAfterInitializationWithConfigurationBeanProviderPathIsNotNull() throws Exception {
    String beanName = CommonBeans.BEAN_BASE_CONF;
    when(credentialPathProvider.newPath()).thenReturn(new Path("/test.json"));
    when(fileSystemFactory.getFileSystem(configuration)).thenReturn(fileSystem);

    processor.postProcessAfterInitialization(configuration, beanName);
    verify(bindGoogleHadoopFileSystem).bindFileSystem(configuration);
    verify(credentialCopier)
        .copyCredentials(fileSystem, configuration, credentialPathProvider, distributedFileSystemPathProvider);
  }

  @Test
  public void postProcessAfterInitializationWithConfigurationBeanProviderPathIsNull() throws Exception {
    String beanName = CommonBeans.BEAN_BASE_CONF;
    when(credentialPathProvider.newPath()).thenReturn(null);

    processor.postProcessAfterInitialization(configuration, beanName);
    verifyZeroInteractions(bindGoogleHadoopFileSystem, credentialCopier, fileSystemFactory);
  }
}
