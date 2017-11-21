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
package com.hotels.bdp.circustrain.api.copier;

import java.util.HashMap;
import java.util.Map;

import com.google.common.collect.ImmutableMap;

import com.hotels.bdp.circustrain.api.metrics.Metrics;

public interface MetricsMerger {

  Metrics merge(Metrics left, Metrics right);

  /**
   * A {@code MetricsMerger} that sums the values of metrics in the intersection of both sets and adds the metrics in
   * the difference of both sets. The number of bytes written is also summarised.
   */
  public static final MetricsMerger DEFAULT = new MetricsMerger() {

    class BasicMetrics implements Metrics {
      private final Map<String, Long> metrics;
      private final long bytesReplicated;

      private BasicMetrics(Map<String, Long> metrics, long bytesReplicated) {
        this.metrics = metrics;
        this.bytesReplicated = bytesReplicated;
      }

      @Override
      public Map<String, Long> getMetrics() {
        return metrics;
      }

      @Override
      public long getBytesReplicated() {
        return bytesReplicated;
      }
    };

    @Override
    public Metrics merge(Metrics left, Metrics right) {
      Map<String, Long> metrics = new HashMap<>();
      metrics.putAll(left.getMetrics());
      for (Map.Entry<String, Long> metricEntry : right.getMetrics().entrySet()) {
        Long currentValue = metrics.get(metricEntry.getKey());
        if (currentValue == null) {
          metrics.put(metricEntry.getKey(), metricEntry.getValue());
        } else {
          metrics.put(metricEntry.getKey(), currentValue + metricEntry.getValue());
        }
      }
      long totalBytesReplicated = left.getBytesReplicated() + right.getBytesReplicated();
      return new BasicMetrics(ImmutableMap.copyOf(metrics), totalBytesReplicated);
    }
  };

}
