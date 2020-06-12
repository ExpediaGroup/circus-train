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
import com.hotels.bdp.circustrain.api.data.DataManipulationClientFactory;
import com.hotels.bdp.circustrain.api.data.DataManipulationClientFactoryManager;

@Profile({ Modules.REPLICATION })
@Component
@Order(Ordered.LOWEST_PRECEDENCE)
public class DefaultDataManipulationClientFactoryManager implements DataManipulationClientFactoryManager {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultDataManipulationClientFactoryManager.class);
  private static final String CLIENT_MANIPULATION_FACTORY_CLASS = "client-manipulation-factory-class";

  private List<DataManipulationClientFactory> clientFactories;

  @Autowired
  public DefaultDataManipulationClientFactoryManager(List<DataManipulationClientFactory> clientFactories) {
    this.clientFactories = clientFactories;
  }

  @PostConstruct
  void postConstruct() {
    LOG.debug("Initialized with {} DataManipulationClientFactories", clientFactories.size());
    for (DataManipulationClientFactory clientFactory : clientFactories) {
      LOG.debug("ClientFactory class {}", clientFactory.getClass().getName());
    }
  }

  @Override
  public DataManipulationClientFactory getClientFactory(
      Path sourceTableLocation,
      Path replicaTableLocation,
      Map<String, Object> copierOptions) {
    System.out.println("here");
    String replicaLocation = replicaTableLocation.toUri().getScheme();
    String sourceLocation = sourceTableLocation.toUri().getScheme();

    // check to see if client factory option has been overridden in the copier options
    System.out.println("copier options: " + copierOptions);
    if (copierOptions.containsKey(CLIENT_MANIPULATION_FACTORY_CLASS)) {
      System.out.println("checking");
      for (DataManipulationClientFactory clientFactory : clientFactories) {
        final String clientFactoryClassName = clientFactory.getClass().getName();
        if (clientFactoryClassName.equals(copierOptions.get(CLIENT_MANIPULATION_FACTORY_CLASS).toString())) {
          LOG.debug("Found ClientFactory '{}' using config", clientFactoryClassName);
          return clientFactory;
        }
      }
    } else {
      for (DataManipulationClientFactory clientFactory : clientFactories) {
        if (clientFactory.supportsSchemes(sourceLocation, replicaLocation)) {
          LOG
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
