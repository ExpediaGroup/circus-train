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
package com.hotels.bdp.circustrain.s3mapreducecp.aws;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;

import com.hotels.bdp.circustrain.aws.AwsDataManipulationClient;
import com.hotels.bdp.circustrain.core.client.DataManipulationClient;
import com.hotels.bdp.circustrain.core.client.DataManipulationClientFactory;

public class AwsDataManipulationClientFactory implements DataManipulationClientFactory {

  private AwsS3ClientFactory s3ClientFactory;

  private final String S3_LOCATION = "s3";
  private final String HDFS_LOCATION = "hdfs";

  private Configuration conf;

  public AwsDataManipulationClientFactory(Configuration conf) {
    // this.s3ClientFactory = s3ClientFactory;
    this.conf = conf;
    s3ClientFactory = new AwsS3ClientFactory();
  }

  // The hdfs -> s3 client doesn't need to use the path for the client
  @Override
  public DataManipulationClient newInstance(String path) {
    return new AwsDataManipulationClient(s3ClientFactory.newInstance(conf));
  }

  /**
   * Checks that the source is an hdfs location and replica location is in S3.
   */
  @Override
  public boolean supportsDeletion(String source, String replica) {
    return (source.toLowerCase().startsWith(HDFS_LOCATION) && replica.toLowerCase().startsWith(S3_LOCATION));
  }

  // The hdfs -> s3 client doesn't need to use the copier options
  @Override
  public void withCopierOptions(Map<String, Object> copierOptions) {}

}
