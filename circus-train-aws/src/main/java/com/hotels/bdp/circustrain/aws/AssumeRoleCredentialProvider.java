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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider.Builder;

public class AssumeRoleCredentialProvider implements AWSCredentialsProvider {

  public static final String ASSUME_ROLE_PROPERTY_NAME = "com.hotels.bdp.circustrain.aws.AssumeRoleCredentialProvider.assumeRole";
  private static final int CREDENTIALS_DURATION = 12 * 60 * 60; // max duration for assumed role credentials

  private AWSCredentials credentials;
  private final Configuration conf;

  public AssumeRoleCredentialProvider(Configuration conf) {
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
    checkNotNull(conf, "conf is required");
    String roleArn = conf.get(ASSUME_ROLE_PROPERTY_NAME);
    checkArgument(StringUtils.isNotEmpty(roleArn),
        "Role ARN must not be empty, please set: " + ASSUME_ROLE_PROPERTY_NAME);

    Builder builder = new STSAssumeRoleSessionCredentialsProvider.Builder(roleArn, "ct-assume-role-session");
    credentials = builder.withRoleSessionDurationSeconds(CREDENTIALS_DURATION).build().getCredentials();
  }

}
