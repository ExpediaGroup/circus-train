/**
 * Copyright (C) 2016-2020 Expedia, Inc.
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

import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;

public class AssumeRoleCredentialProvider implements AWSCredentialsProvider {

  public static final String ASSUME_ROLE_PROPERTY_NAME = "com.hotels.bdp.circustrain.aws.AssumeRoleCredentialProvider.assumeRole";
  public static final String ASSUME_ROLE_SESSION_DURATION_SECONDS_PROPERTY_NAME = "com.hotels.bdp.circustrain.aws.AssumeRoleCredentialProvider.assumeRoleSessionDurationSeconds";
  private static final int DEFAULT_CREDENTIALS_DURATION = (int) TimeUnit.HOURS.toSeconds(12); // max duration in seconds for assumed role credentials

  private final Configuration conf;
  private STSAssumeRoleSessionCredentialsProvider credentialsProvider;

  public AssumeRoleCredentialProvider(Configuration conf) {
    this.conf = conf;
  }

  private void initializeCredentialProvider() {
    String roleArn = conf.get(ASSUME_ROLE_PROPERTY_NAME);
    checkArgument(StringUtils.isNotEmpty(roleArn),
        "Role ARN must not be empty, please set: " + ASSUME_ROLE_PROPERTY_NAME);

    int roleSessionDuration = conf.getInt(ASSUME_ROLE_SESSION_DURATION_SECONDS_PROPERTY_NAME, DEFAULT_CREDENTIALS_DURATION);

    // STSAssumeRoleSessionCredentialsProvider should auto refresh its credentials in the background.
    this.credentialsProvider = new STSAssumeRoleSessionCredentialsProvider
        .Builder(roleArn, "ct-assume-role-session")
        .withRoleSessionDurationSeconds(roleSessionDuration)
        .build();
  }

  @Override
  public AWSCredentials getCredentials() {
    if (this.credentialsProvider == null) {
      initializeCredentialProvider();
    }
    return this.credentialsProvider.getCredentials();
  }

  @Override
  public void refresh() {
    if (this.credentialsProvider == null) {
      initializeCredentialProvider();
    }

    this.credentialsProvider.refresh();
  }

}
