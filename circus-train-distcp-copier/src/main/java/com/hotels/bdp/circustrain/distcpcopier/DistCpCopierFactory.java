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

import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import com.codahale.metrics.MetricRegistry;

import com.hotels.bdp.circustrain.api.Modules;
import com.hotels.bdp.circustrain.api.copier.Copier;
import com.hotels.bdp.circustrain.api.copier.CopierContext;
import com.hotels.bdp.circustrain.api.copier.CopierFactory;

@Profile({ Modules.REPLICATION })
@Component
@Order(Ordered.LOWEST_PRECEDENCE) // This will give priority to user-defined CopierFactory's
public class DistCpCopierFactory implements CopierFactory {

  private final Configuration conf;
  private final MetricRegistry runningMetricsRegistry;

  @Autowired
  DistCpCopierFactory(@Value("#{sourceHiveConf}") Configuration conf, MetricRegistry runningMetricsRegistry) {
    this.conf = conf;
    this.runningMetricsRegistry = runningMetricsRegistry;
  }

  @Override
  public boolean supportsSchemes(String sourceScheme, String replicaScheme) {
    return true;
  }

  @Override
  public Copier newInstance(CopierContext copierContext) {
    return new DistCpCopier(conf, copierContext.getSourceBaseLocation(), copierContext.getSourceSubLocations(),
        copierContext.getReplicaLocation(), copierContext.getCopierOptions(), runningMetricsRegistry);
  }

  // TODO: difference between null and Collections.<Path>emptyList() for sourceSubLocations
  @Override
  public Copier newInstance(
      String eventId,
      Path sourceBaseLocation,
      Path replicaLocation,
      Map<String, Object> copierOptions) {
    CopierContext copierContext = new CopierContext(eventId, sourceBaseLocation, replicaLocation, copierOptions);
    return newInstance(copierContext);
  }

  @Override
  public Copier newInstance(
      String eventId,
      Path sourceBaseLocation,
      List<Path> sourceSubLocations,
      Path replicaLocation,
      Map<String, Object> copierOptions) {
    CopierContext copierContext = new CopierContext(eventId, sourceBaseLocation, sourceSubLocations, replicaLocation,
        copierOptions);
    return newInstance(copierContext);
  }

}
