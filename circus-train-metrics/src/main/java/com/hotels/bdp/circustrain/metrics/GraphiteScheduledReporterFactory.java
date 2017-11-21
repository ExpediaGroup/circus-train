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
package com.hotels.bdp.circustrain.metrics;

import java.net.InetSocketAddress;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.graphite.Graphite;
import com.codahale.metrics.graphite.GraphiteReporter;

import com.hotels.bdp.circustrain.api.metrics.ScheduledReporterFactory;
import com.hotels.bdp.circustrain.api.util.DotJoiner;

public class GraphiteScheduledReporterFactory implements ScheduledReporterFactory {

  private final MetricRegistry runningMetricRegistry;
  private final String graphiteHost;
  private final String graphitePrefix;

  public GraphiteScheduledReporterFactory(
      MetricRegistry runningMetricRegistry,
      String graphiteHost,
      String graphitePrefix) {
    this.runningMetricRegistry = runningMetricRegistry;
    this.graphiteHost = graphiteHost;
    this.graphitePrefix = graphitePrefix;
  }

  @Override
  public ScheduledReporter newInstance(String qualifiedReplicaName) {
    InetSocketAddress address = new InetSocketAddressFactory().newInstance(graphiteHost);
    Graphite graphite = new Graphite(address);
    String prefix = DotJoiner.join(graphitePrefix, qualifiedReplicaName);
    return GraphiteReporter.forRegistry(runningMetricRegistry).prefixedWith(prefix).build(graphite);
  }

}
