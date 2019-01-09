/**
 * Copyright (C) 2016-2019 Expedia Inc.
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
package com.hotels.bdp.circustrain.core.event;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.codahale.metrics.ScheduledReporter;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import com.google.common.collect.Maps;

import com.hotels.bdp.circustrain.api.CompletionCode;
import com.hotels.bdp.circustrain.api.event.CopierListener;
import com.hotels.bdp.circustrain.api.event.EventTableReplication;
import com.hotels.bdp.circustrain.api.event.TableReplicationListener;
import com.hotels.bdp.circustrain.api.metrics.MetricSender;
import com.hotels.bdp.circustrain.api.metrics.Metrics;
import com.hotels.bdp.circustrain.api.metrics.ScheduledReporterFactory;
import com.hotels.bdp.circustrain.api.util.DotJoiner;

@Component
class MetricsListener implements TableReplicationListener, CopierListener {

  private final Map<String, Long> startTimes = new HashMap<>();
  private final MetricSender metricSender;
  private String qualifiedReplicaName;
  private Metrics metrics;
  private ScheduledReporter runningMetricsReporter;
  private final ScheduledReporterFactory runningMetricsReporterFactory;
  private final long metricsReporterPeriod;
  private final TimeUnit metricsReporterTimeUnit;

  @Autowired
  MetricsListener(
      MetricSender metricSender,
      ScheduledReporterFactory runningMetricsReporterFactory,
      @Value("${metrics-reporter.period:1}") long metricsReporterPeriod,
      @Value("${metrics-reporter.time-unit:MINUTES}") TimeUnit metricsReporterTimeUnit) {
    this.metricSender = metricSender;
    this.runningMetricsReporterFactory = runningMetricsReporterFactory;
    this.metricsReporterPeriod = metricsReporterPeriod;
    this.metricsReporterTimeUnit = metricsReporterTimeUnit;
  }

  @Override
  public void tableReplicationStart(EventTableReplication tableReplication, String eventId) {
    qualifiedReplicaName = tableReplication.getQualifiedReplicaName();
    startTimes.put(qualifiedReplicaName, System.currentTimeMillis());
  }

  @Override
  public void tableReplicationSuccess(EventTableReplication tableReplication, String eventId) {
    sendMetrics(CompletionCode.SUCCESS, tableReplication.getQualifiedReplicaName(), metrics);
  }

  @Override
  public void tableReplicationFailure(EventTableReplication tableReplication, String eventId, Throwable t) {
    sendMetrics(CompletionCode.FAILURE, tableReplication.getQualifiedReplicaName(), Metrics.NULL_VALUE);
  }

  @Override
  public void copierEnd(Metrics metrics) {
    if (runningMetricsReporter == null) {
      throw new IllegalStateException("Metrics should not be null");
    }
    runningMetricsReporter.report();
    runningMetricsReporter.stop();
    // once stopped unusable so get rid of it
    runningMetricsReporter = null;
    this.metrics = metrics;
  }

  private void sendMetrics(CompletionCode completionCode, String target, Metrics metrics) {
    Builder<String, Long> builder = ImmutableMap.builder();
    builder.put(replicationTime(target));
    builder.put(completionCode(target, completionCode));

    if (metrics != null) {
      builder.put(bytesReplicated(target, metrics));
      for (Entry<String, Long> metric : metrics.getMetrics().entrySet()) {
        builder.put(DotJoiner.join(target, metric.getKey()), metric.getValue());
      }
    }

    metricSender.send(builder.build());
  }

  private Entry<String, Long> replicationTime(String target) {
    Long startTime = startTimes.remove(target);
    long replicationTime = -1L;
    if (startTime != null) {
      replicationTime = System.currentTimeMillis() - startTime;
    }
    return Maps.immutableEntry(DotJoiner.join(target, "replication_time"), replicationTime);
  }

  private Entry<String, Long> completionCode(String target, CompletionCode completionCode) {
    return Maps.immutableEntry(DotJoiner.join(target, completionCode.getMetricName()), completionCode.getCode());
  }

  private Entry<String, Long> bytesReplicated(String target, Metrics metrics) {
    return Maps.immutableEntry(DotJoiner.join(target, "bytes_replicated"), metrics.getBytesReplicated());
  }

  @Override
  public void copierStart(String copierImplementation) {
    runningMetricsReporter = runningMetricsReporterFactory.newInstance(qualifiedReplicaName);
    runningMetricsReporter.start(metricsReporterPeriod, metricsReporterTimeUnit);
    runningMetricsReporter.report();
  }

}
