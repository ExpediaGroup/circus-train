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
package com.hotels.bdp.circustrain.core;

import java.util.List;

import javax.annotation.PostConstruct;

import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import com.google.common.collect.ImmutableList;

import com.hotels.bdp.circustrain.api.Modules;
import com.hotels.bdp.circustrain.api.copier.CopierFactory;

@Profile({ Modules.REPLICATION })
@Component
@Order(Ordered.LOWEST_PRECEDENCE)
public class CopierFactoryManager {

  private static final Logger LOG = LoggerFactory.getLogger(CopierFactoryManager.class);

  private final List<CopierFactory> copierFactories;

  @Autowired
  public CopierFactoryManager(List<CopierFactory> copierFactories) {
    this.copierFactories = ImmutableList.copyOf(copierFactories);
  }

  @PostConstruct
  void postConstruct() {
    LOG.debug("Initialized with {} CopierFactories", copierFactories.size());
    for (CopierFactory copierFactory : copierFactories) {
      LOG.debug("CopierFactory class {}", copierFactory.getClass().getName());
    }
  }

  CopierFactory getCopierFactory(Path sourceLocation, Path replicaLocation) {
    String sourceScheme = sourceLocation.toUri().getScheme();
    String replicaScheme = replicaLocation.toUri().getScheme();
    for (CopierFactory copierFactory : copierFactories) {
      if (copierFactory.supportsSchemes(sourceScheme, replicaScheme)) {
        LOG.debug("Found CopierFactory '{}' for sourceScheme '{}' and replicaScheme '{}'",
            copierFactory.getClass().getName(), sourceScheme, replicaScheme);
        return copierFactory;
      }
    }
    throw new UnsupportedOperationException("No CopierFactory that suppports sourceScheme '"
        + sourceScheme
        + "' and replicaScheme '"
        + replicaScheme
        + "'");
  }

}
