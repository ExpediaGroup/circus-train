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

public class AssumeRoleCredentialProvider implements AWSCredentialsProvider {
    // TODO: pass in from ct.yml
    private static final String ROLE_ARN = "arn:aws:iam::826353747357:role/jetstream-write-apiary-s3-role";

    public AssumeRoleCredentialProvider() {

    }

    public AWSCredentials getCredentials() {
        return getAssumedRoleCredentials(ROLE_ARN);
    }

    public void refresh() {
    }

    public AWSCredentials getAssumedRoleCredentials(String roleARN) {
        STSAssumeRoleSessionCredentialsProvider.Builder builder =
                new STSAssumeRoleSessionCredentialsProvider.Builder(roleARN, "ct-assume-role-session");

        AWSCredentials credentials = builder
                .withRoleSessionDurationSeconds(12 * 60 * 60) // max duration for assumed role credentials
                .build()
                .getCredentials();
        return credentials;
    }
}
