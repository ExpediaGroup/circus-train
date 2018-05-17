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
package com.hotels.bdp.circustrain.aws.context;

import static org.apache.hadoop.security.alias.CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH;

import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.stereotype.Component;

import com.hotels.bdp.circustrain.aws.BindS3AFileSystem;
import com.hotels.bdp.circustrain.aws.S3CredentialsUtils;
import com.hotels.bdp.circustrain.context.CommonBeans;

@Component
public class AWSBeanPostProcessor implements BeanPostProcessor {

  private final BindS3AFileSystem bindS3AFileSystem;
  private final S3CredentialsUtils s3CredentialsUtils;

  @Autowired
  public AWSBeanPostProcessor(BindS3AFileSystem bindS3AFileSystem, S3CredentialsUtils s3CredentialsUtils) {
    this.bindS3AFileSystem = bindS3AFileSystem;
    this.s3CredentialsUtils = s3CredentialsUtils;
  }

  @Override
  public Object postProcessBeforeInitialization(Object bean, String beanName) throws BeansException {
    return bean;
  }

  @Override
  public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
    if (CommonBeans.BEAN_BASE_CONF.equals(beanName)) {
      Configuration baseConf = (Configuration) bean;
      bindS3AFileSystem.bindFileSystem(baseConf);
      if (baseConf.get(CREDENTIAL_PROVIDER_PATH) != null) {
        s3CredentialsUtils.setS3Credentials(baseConf);
      }
      return baseConf;
    }
    return bean;
  }

}
