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

import static java.util.Collections.singletonMap;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Clock;
import com.codahale.metrics.graphite.Graphite;
import com.codahale.metrics.graphite.GraphiteSender;

import com.hotels.bdp.circustrain.api.metrics.MetricSender;
import com.hotels.bdp.circustrain.api.util.DotJoiner;

public class GraphiteMetricSender implements MetricSender {

  private static final Logger LOG = LoggerFactory.getLogger(GraphiteMetricSender.class);

  private final GraphiteSender graphite;
  private final Clock clock;
  private final String prefix;

  public static GraphiteMetricSender newInstance(String host, String prefix) {
    InetSocketAddress address = new InetSocketAddressFactory().newInstance(host);
    Graphite graphite = new Graphite(address);
    return new GraphiteMetricSender(graphite, Clock.defaultClock(), prefix);
  }

  GraphiteMetricSender(GraphiteSender graphite, Clock clock, String prefix) {
    this.graphite = graphite;
    this.clock = clock;
    this.prefix = prefix;
  }

  @Override
  public void send(String name, long value) {
    send(singletonMap(name, value));
  }

  @Override
  public void send(Map<String, Long> metrics) {
    MetricSender.DEFAULT_LOG_ONLY.send(metrics);
    long timestamp = MILLISECONDS.toSeconds(clock.getTime());
    try {
      if (!graphite.isConnected()) {
        graphite.connect();
      }
      for (Entry<String, Long> metric : metrics.entrySet()) {
        String metricPath = DotJoiner.join(prefix, metric.getKey());
        String metricValue = Long.toString(metric.getValue());
        LOG.debug("Sending metric {} {} {}", metricPath, metricValue, timestamp);
        graphite.send(metricPath, metricValue, timestamp);
      }
      graphite.flush();
    } catch (IOException e) {
      LOG.warn("Unable to report to Graphite", e);
    } finally {
      try {
        graphite.close();
      } catch (IOException e) {
        LOG.warn("Error closing Graphite", e);
      }
    }
  }

}
