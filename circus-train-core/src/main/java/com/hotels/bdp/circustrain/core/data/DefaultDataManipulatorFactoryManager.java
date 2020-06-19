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
  static final String DATA_MANIPULATOR_FACTORY_CLASS = "data-manipulator-factory-class";

  private List<DataManipulatorFactory> dataManipulatorFactories;

  @Autowired
  public DefaultDataManipulatorFactoryManager(List<DataManipulatorFactory> factories) {
    this.dataManipulatorFactories = factories;
  }

  @PostConstruct
  void postConstruct() {
    log.debug("Initialized with {} DataManipulatorFactories", dataManipulatorFactories.size());
    for (DataManipulatorFactory factory : dataManipulatorFactories) {
      log.debug("DataManipulatorFactory class {}", factory.getClass().getName());
    }
  }

  @Override
  public DataManipulatorFactory getFactory(
      Path sourceTableLocation,
      Path replicaTableLocation,
      Map<String, Object> copierOptions) {
    String replicaLocation = replicaTableLocation.toUri().getScheme();
    String sourceLocation = sourceTableLocation.toUri().getScheme();

    if (copierOptions.containsKey(DATA_MANIPULATOR_FACTORY_CLASS)) {
      for (DataManipulatorFactory factory : dataManipulatorFactories) {
        final String factoryClassName = factory.getClass().getName();
        if (factoryClassName.equals(copierOptions.get(DATA_MANIPULATOR_FACTORY_CLASS).toString())) {
          log.debug("Found DataManipulatorFactory '{}' using config", factoryClassName);
          return factory;
        }
      }
    } else {
      for (DataManipulatorFactory factory : dataManipulatorFactories) {
        if (factory.supportsSchemes(sourceLocation, replicaLocation)) {
          log
              .debug("Found DataManipulatorFactory {} for cleanup at location {}.", factory.getClass().getName(),
                  replicaLocation);
          return factory;
        }
      }
    }
    throw new UnsupportedOperationException(
        "No DataManipulator found which can delete the data at location: " + replicaLocation);
  }

}
