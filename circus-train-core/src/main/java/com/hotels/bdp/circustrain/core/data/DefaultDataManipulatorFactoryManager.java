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
package com.hotels.bdp.circustrain.core.data;

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
import com.hotels.bdp.circustrain.api.data.DataManipulatorFactory;
import com.hotels.bdp.circustrain.api.data.DataManipulatorFactoryManager;

@Profile({ Modules.REPLICATION })
@Component
@Order(Ordered.LOWEST_PRECEDENCE)
public class DefaultDataManipulatorFactoryManager implements DataManipulatorFactoryManager {

  private static final Logger log = LoggerFactory.getLogger(DefaultDataManipulatorFactoryManager.class);
  private static final String DATA_MANIPULATION_FACTORY_CLASS = "data-manipulation-factory-class";

  private List<DataManipulatorFactory> clientFactories;

  @Autowired
  public DefaultDataManipulatorFactoryManager(List<DataManipulatorFactory> clientFactories) {
    this.clientFactories = clientFactories;
  }

  @PostConstruct
  void postConstruct() {
    log.debug("Initialized with {} DataManipulationClientFactories", clientFactories.size());
    for (DataManipulatorFactory clientFactory : clientFactories) {
      log.debug("ClientFactory class {}", clientFactory.getClass().getName());
    }
  }

  @Override
  public DataManipulatorFactory getClientFactory(
      Path sourceTableLocation,
      Path replicaTableLocation,
      Map<String, Object> copierOptions) {
    String replicaLocation = replicaTableLocation.toUri().getScheme();
    String sourceLocation = sourceTableLocation.toUri().getScheme();

    if (copierOptions.containsKey(DATA_MANIPULATION_FACTORY_CLASS)) {
      for (DataManipulatorFactory clientFactory : clientFactories) {
        final String clientFactoryClassName = clientFactory.getClass().getName();
        if (clientFactoryClassName.equals(copierOptions.get(DATA_MANIPULATION_FACTORY_CLASS).toString())) {
          log.debug("Found ClientFactory '{}' using config", clientFactoryClassName);
          return clientFactory;
        }
      }
    } else {
      for (DataManipulatorFactory clientFactory : clientFactories) {
        if (clientFactory.supportsSchemes(sourceLocation, replicaLocation)) {
          log
              .debug("Found client factory {} for cleanup at location {}.", clientFactory.getClass().getName(),
                  replicaLocation);
          return clientFactory;
        }
      }
    }
    throw new UnsupportedOperationException(
        "No DataManipulationClient found which can delete the data at location: " + replicaLocation);
  }

}
