/**
 * Copyright (C) 2016-2019 Expedia Inc.
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

import org.apache.hadoop.conf.Configuration;

import com.amazonaws.auth.AWSCredentialsProviderChain;
import com.amazonaws.auth.EC2ContainerCredentialsProviderWrapper;

/**
 * AWS credentials provider chain that looks for credentials in this order:
 * <ul>
 * <li>Credentials from Hadoop configuration set via JCE KS - if a JCE KS path or Hadoop {@code Configuration} is
 * provided</li>
 * <li>{@link EC2ContainerCredentialsProviderWrapper} that loads credentials from an Amazon Container (e.g. EC2)</li>
 * </ul>
 *
 * @see JceksAWSCredentialProvider
 * @see EC2ContainerCredentialsProviderWrapper
 */
public class HadoopAWSCredentialProviderChain extends AWSCredentialsProviderChain {

  public HadoopAWSCredentialProviderChain() {
    super(new EC2ContainerCredentialsProviderWrapper());
  }

  public HadoopAWSCredentialProviderChain(String credentialProviderPath) {
    this(AWSCredentialUtils.configureCredentialProvider(credentialProviderPath));
  }

  public HadoopAWSCredentialProviderChain(Configuration conf) {
    super(new JceksAWSCredentialProvider(conf), new EC2ContainerCredentialsProviderWrapper());
  }

}
