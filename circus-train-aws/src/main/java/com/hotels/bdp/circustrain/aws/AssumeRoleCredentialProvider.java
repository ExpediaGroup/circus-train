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

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
import org.apache.commons.lang.NullArgumentException;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;

import static com.google.common.base.Preconditions.checkNotNull;

public class AssumeRoleCredentialProvider implements AWSCredentialsProvider {
    private static final String ASSUME_ROLE_PROPERTY_NAME = "com.hotels.bdp.circustrain.s3mapreducecp.assumeRole";
    private static final int CREDENTIALS_DURATION = 12 * 60 * 60; // max duration for assumed role credentials

    private AWSCredentials credentials;
    private String roleArn;


    public AssumeRoleCredentialProvider(Configuration conf) {
        checkNotNull(conf, "conf is required");
        roleArn = conf.get(ASSUME_ROLE_PROPERTY_NAME);
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
        credentials = getAssumedRoleCredentials(this.roleArn);
    }

    public AWSCredentials getAssumedRoleCredentials(String roleArn) {
        if (StringUtils.isEmpty(roleArn)) {
            throw new NullArgumentException("Role ARN must not be empty");
        }

        STSAssumeRoleSessionCredentialsProvider.Builder builder =
                new STSAssumeRoleSessionCredentialsProvider.Builder(roleArn, "ct-assume-role-session");

        credentials = builder
                .withRoleSessionDurationSeconds(CREDENTIALS_DURATION)
                .build()
                .getCredentials();

        return credentials;
    }
}
