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
package com.hotels.bdp.circustrain.aws.sns.event;

import static java.lang.String.format;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.util.concurrent.Executors;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSCredentialsProviderChain;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.retry.PredefinedRetryPolicies;
import com.amazonaws.services.sns.AmazonSNSAsyncClient;

@Configuration
public class SnsConfiguration {

  @Bean
  AmazonSNSAsyncClient amazonSNS(ListenerConfig config, AWSCredentialsProvider awsCredentialsProvider) {
    AmazonSNSAsyncClient client = new AmazonSNSAsyncClient(awsCredentialsProvider,
        new ClientConfiguration().withRetryPolicy(PredefinedRetryPolicies.getDefaultRetryPolicy()),
        Executors.newSingleThreadScheduledExecutor());
    client.setRegion(Region.getRegion(Regions.fromName(config.getRegion())));
    return client;
  }

  @Bean
  AWSCredentialsProvider awsCredentialsProvider(
      @Qualifier("replicaHiveConf") org.apache.hadoop.conf.Configuration conf) {
    return new AWSCredentialsProviderChain(new BasicAuth(conf), InstanceProfileCredentialsProvider.getInstance());
  }

  static private class BasicAuth implements AWSCredentialsProvider {

    private final org.apache.hadoop.conf.Configuration conf;

    public BasicAuth(org.apache.hadoop.conf.Configuration conf) {
      this.conf = conf;
    }

    @Override
    public AWSCredentials getCredentials() {
      return new BasicAWSCredentials(getKey(conf, "access.key"), getKey(conf, "secret.key"));
    }

    @Override
    public void refresh() {}

    private static String getKey(org.apache.hadoop.conf.Configuration conf, String keyType) {
      checkNotNull(conf, "conf cannot be null");
      checkNotNull(keyType, "KeyType cannot be null");

      try {
        char[] key = conf.getPassword(keyType);
        if (key == null) {
          throw new IllegalStateException(format("Unable to get value of '%s'", keyType));
        }
        return new String(key);
      } catch (IOException e) {
        throw new IllegalStateException(format("Error getting key for '%s' from credential provider", keyType), e);
      }
    }
  }
}
