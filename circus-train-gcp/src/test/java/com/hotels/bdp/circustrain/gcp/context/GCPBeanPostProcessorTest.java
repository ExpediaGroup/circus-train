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
package com.hotels.bdp.circustrain.gcp.context;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Spy;
import org.mockito.runners.MockitoJUnitRunner;

import com.hotels.bdp.circustrain.context.CommonBeans;

@RunWith(MockitoJUnitRunner.class)
public class GCPBeanPostProcessorTest {
  private @Spy GCPSecurity security = new GCPSecurity();
  private @Spy Configuration configuration = new Configuration();

  @Test
  public void postProcessAfterInitializationWithCorrectBeanName() throws Exception {
    String beanName = CommonBeans.BEAN_BASE_CONF;
    GCPBeanPostProcessor processor = new GCPBeanPostProcessor(security);
    processor.postProcessAfterInitialization(configuration, beanName);
    verify(security, times(1)).getCredentialProvider();
  }

  @Test
  public void postProcessAfterInitializationWithIncorrectBeanName() throws Exception {
    String beanName = "notBaseConf";
    GCPBeanPostProcessor processor = new GCPBeanPostProcessor(security);
    processor.postProcessAfterInitialization(configuration, beanName);
    verify(security, times(0)).getCredentialProvider();
  }

  @Test
  public void postProcessAfterInitializationWithBlankCredentialProviderDoesntModifyConfiguration() throws Exception {
    String beanName = CommonBeans.BEAN_BASE_CONF;
    GCPBeanPostProcessor processor = new GCPBeanPostProcessor(security);
    processor.postProcessAfterInitialization(configuration, beanName);
    when(security.getCredentialProvider()).thenReturn("");
    verify(security, times(1)).getCredentialProvider();
    verify(configuration, times(0)).set(anyString(), anyString());
  }
}