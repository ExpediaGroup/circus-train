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
package com.hotels.bdp.circustrain.core.client;

import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;

import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import com.hotels.bdp.circustrain.api.Modules;

@Profile({ Modules.REPLICATION })
@Component
@Order(Ordered.LOWEST_PRECEDENCE)
public class DataManipulationClientFactoryManager {

  private static final Logger LOG = LoggerFactory.getLogger(DataManipulationClientFactoryManager.class);

  private List<DataManipulationClientFactory> clientFactories;
  private Map<String, Object> copierOptions;
  private String sourceLocation;

  @Autowired
  public DataManipulationClientFactoryManager(List<DataManipulationClientFactory> clientFactories) {
    this.clientFactories = clientFactories;
  }

  @PostConstruct
  void postConstruct() {
    LOG.debug("Initialized with {} DataManipulationClientFactories", clientFactories.size());
    for (DataManipulationClientFactory clientFactory : clientFactories) {
      LOG.debug("ClientFactory class {}", clientFactory.getClass().getName());
    }
  }

  public DataManipulationClient getClientForPath(String replicaLocation) {
    for (DataManipulationClientFactory clientFactory : clientFactories) {
      if (clientFactory.supportsDeletion(sourceLocation, replicaLocation)) {
        LOG
            .debug("Found client factory {} for cleanup at location {}.", clientFactory.getClass().getName(),
                replicaLocation);
        clientFactory.withCopierOptions(copierOptions);
        return clientFactory.newInstance(replicaLocation);
      }
    }
    throw new UnsupportedOperationException(
        "No DataManipulationClient found which can delete the data at location: " + replicaLocation);
  }

  public void withCopierOptions(Map<String, Object> copierOptions) {
    this.copierOptions = copierOptions;
  }

  public void withSourceLocation(Path sourceLocation) {
    this.sourceLocation = sourceLocation.toString();
  }
}
