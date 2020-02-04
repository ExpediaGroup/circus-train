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
package com.hotels.bdp.circustrain.aws;

import static com.google.common.base.Preconditions.checkNotNull;

import org.apache.hadoop.conf.Configuration;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;

/**
 * A {@code AWSCredentialsProvider} implementation that takes the credentials from a JCE KS file and is integrated with
 * Hadoop framework.
 */
class JceksAWSCredentialProvider implements AWSCredentialsProvider {

  private AWSCredentials credentials;
  private final Configuration conf;

  JceksAWSCredentialProvider(String credentialProviderPath) {
    this(AWSCredentialUtils.configureCredentialProvider(credentialProviderPath));
  }

  JceksAWSCredentialProvider(Configuration conf) {
    checkNotNull(conf, "conf is required");
    this.conf = conf;
  }

  @Override
  public AWSCredentials getCredentials() {
    if (credentials == null) {
      refresh();
    }
    return credentials;
  }

  @Override
  public void refresh() {
    credentials = new BasicAWSCredentials(AWSCredentialUtils.getKey(conf, AWSConstants.ACCESS_KEY),
        AWSCredentialUtils.getKey(conf, AWSConstants.SECRET_KEY));
  }

}
