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
package com.hotels.bdp.circustrain.distcpcopier;

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

@Profile({ Modules.REPLICATION })
@Component
@Order(Ordered.LOWEST_PRECEDENCE)
public class HdfsDataManipulationClientFactory implements DataManipulationClientFactory {

  private Configuration conf;

  @Autowired
  public HdfsDataManipulationClientFactory(@Value("#{replicaHiveConf}") Configuration conf) {
    this.conf = conf;
  }

  // The HDFS client doesn't need to use the path.
  @Override
  public DataManipulationClient newInstance(String path) {
    return new HdfsDataManipulationClient(conf);
  }

  /**
   * This will delete replica data whether it has been replicated from s3 or hdfs.
   */
  @Override
  public boolean supportsSchemes(String sourceScheme, String replicaScheme) {
    return true;
  }

  // The HDFS client doesn't need to use the copier options.
  @Override
  public void withCopierOptions(Map<String, Object> copierOptions) {}

}
