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

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.Region;

import com.hotels.bdp.circustrain.s3mapreducecp.ConfigurationVariable;

public class AwsS3ClientFactoryTest {

  private AwsS3ClientFactory factory = new AwsS3ClientFactory();

  @Test
  public void typical() {
    Configuration conf = new Configuration();
    conf.set(ConfigurationVariable.REGION.getName(), "eu-west-1");
    conf.setInt(ConfigurationVariable.UPLOAD_RETRY_COUNT.getName(), 7);
    conf.setLong(ConfigurationVariable.UPLOAD_RETRY_DELAY_MS.getName(), 333L);
    AmazonS3 client = factory.newInstance(conf);
    assertThat(client, is(instanceOf(AmazonS3Client.class)));
    assertThat(client.getRegion(), is(Region.EU_Ireland));
  }

}
