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
package com.hotels.bdp.circustrain.s3mapreducecp.aws;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.regions.Regions;
import com.amazonaws.retry.RetryPolicy;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

import com.hotels.bdp.circustrain.aws.HadoopAWSCredentialProviderChain;
import com.hotels.bdp.circustrain.s3mapreducecp.ConfigurationVariable;

public class AwsS3ClientFactory {
  private static final Logger LOG = LoggerFactory.getLogger(AwsS3ClientFactory.class);

  private static String getRegion(Configuration conf) {
    String region = conf.get(ConfigurationVariable.REGION.getName());
    if (region == null) {
      region = Regions.DEFAULT_REGION.getName();
    }
    return region;
  }

  private static EndpointConfiguration getEndpointConfiguration(Configuration conf) {
    String endpointUrl = conf.get(ConfigurationVariable.S3_ENDPOINT_URI.getName());
    if (endpointUrl == null) {
      return null;
    }
    return new EndpointConfiguration(endpointUrl, getRegion(conf));
  }

  public AmazonS3 newInstance(Configuration conf) {
    int maxErrorRetry = conf.getInt(ConfigurationVariable.UPLOAD_RETRY_COUNT.getName(),
        ConfigurationVariable.UPLOAD_RETRY_COUNT.defaultIntValue());
    long errorRetryDelay = conf.getLong(ConfigurationVariable.UPLOAD_RETRY_DELAY_MS.getName(),
        ConfigurationVariable.UPLOAD_RETRY_DELAY_MS.defaultLongValue());

    LOG.info("Creating AWS S3 client with a retry policy of {} retries and {} ms of exponential backoff delay",
        maxErrorRetry, errorRetryDelay);

    RetryPolicy retryPolicy = new RetryPolicy(new CounterBasedRetryCondition(maxErrorRetry),
        new ExponentialBackoffStrategy(errorRetryDelay), maxErrorRetry, true);
    ClientConfiguration clientConfiguration = new ClientConfiguration();
    clientConfiguration.setRetryPolicy(retryPolicy);
    clientConfiguration.setMaxErrorRetry(maxErrorRetry);

    AmazonS3ClientBuilder builder = AmazonS3ClientBuilder
        .standard()
        .withCredentials(new HadoopAWSCredentialProviderChain(conf))
        .withClientConfiguration(clientConfiguration);

    EndpointConfiguration endpointConfiguration = getEndpointConfiguration(conf);
    if (endpointConfiguration != null) {
      builder.withEndpointConfiguration(endpointConfiguration);
    } else {
      builder.withRegion(getRegion(conf));
    }

    return builder.build();
  }

}
