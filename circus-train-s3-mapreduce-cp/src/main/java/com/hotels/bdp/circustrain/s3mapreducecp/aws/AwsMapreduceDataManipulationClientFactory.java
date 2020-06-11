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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import com.hotels.bdp.circustrain.api.Modules;
import com.hotels.bdp.circustrain.api.data.DataManipulationClient;
import com.hotels.bdp.circustrain.api.data.DataManipulationClientFactory;
import com.hotels.bdp.circustrain.aws.AwsDataManipulationClient;
import com.hotels.bdp.circustrain.aws.S3Schemes;

@Profile({ Modules.REPLICATION })
@Component
@Order(Ordered.LOWEST_PRECEDENCE - 1)
public class AwsMapreduceDataManipulationClientFactory implements DataManipulationClientFactory {

  private AwsS3ClientFactory s3ClientFactory;

  private Configuration conf;

  @Autowired
  public AwsMapreduceDataManipulationClientFactory(@Value("#{replicaHiveConf}") Configuration conf) {
    this.conf = conf;
    s3ClientFactory = new AwsS3ClientFactory();
  }

  // The hdfs -> s3 client doesn't need to use the path for the client.
  @Override
  public DataManipulationClient newInstance(String path) {
    return new AwsDataManipulationClient(s3ClientFactory.newInstance(conf));
  }

  /**
   * Supports copying from hdfs/ a file to s3.
   */
  @Override
  public boolean supportsSchemes(String sourceScheme, String replicaScheme) {

    System.out.println("source is s3: " + S3Schemes.isS3Scheme(sourceScheme));
    System.out.println("replica is s3: " + S3Schemes.isS3Scheme(replicaScheme));

    return !S3Schemes.isS3Scheme(sourceScheme) && S3Schemes.isS3Scheme(replicaScheme);
  }

  // The hdfs -> s3 client doesn't need to use the copier options.
  @Override
  public void withCopierOptions(Map<String, Object> copierOptions) {}

}
