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
package com.hotels.bdp.circustrain.core.event;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.codahale.metrics.ScheduledReporter;
import com.google.common.collect.ImmutableMap;

import com.hotels.bdp.circustrain.api.event.EventTableReplication;
import com.hotels.bdp.circustrain.api.metrics.MetricSender;
import com.hotels.bdp.circustrain.api.metrics.Metrics;
import com.hotels.bdp.circustrain.api.metrics.ScheduledReporterFactory;

@RunWith(MockitoJUnitRunner.class)
public class MetricsListenerTest {

  private static final String TARGET = "target";

  @Mock
  private MetricSender metricSender;
  @Mock
  private EventTableReplication tableReplication;
  @Mock
  private Metrics metrics;
  @Mock
  private ScheduledReporterFactory scheduledReporterFactory;
  @Mock
  private ScheduledReporter scheduledReporter;

  private MetricsListener listener;

  @Captor
  private ArgumentCaptor<Map<String, Long>> metricsCaptor;

  @Before
  public void before() {
    when(tableReplication.getQualifiedReplicaName()).thenReturn(TARGET);
    when(scheduledReporterFactory.newInstance(anyString())).thenReturn(scheduledReporter);

    listener = new MetricsListener(metricSender, scheduledReporterFactory, 2, TimeUnit.SECONDS);
  }

  @Test
  public void typicalSuccess() {
    when(metrics.getMetrics()).thenReturn(ImmutableMap.<String, Long> of("foo", 42L));
    when(metrics.getBytesReplicated()).thenReturn(13L);

    listener.tableReplicationStart(tableReplication, "eventId");
    listener.copierStart("");
    listener.copierEnd(metrics);
    listener.tableReplicationSuccess(tableReplication, "event-id");

    verify(scheduledReporter).start(2, TimeUnit.SECONDS);
    verify(scheduledReporter, times(2)).report();
    verify(scheduledReporter).stop();
    verify(metricSender).send(metricsCaptor.capture());

    Map<String, Long> metrics = metricsCaptor.getValue();

    assertThat(metrics.size(), is(4));
    assertThat(metrics.get("target.replication_time"), greaterThanOrEqualTo(0L));
    assertThat(metrics.get("target.completion_code"), is(1L));
    assertThat(metrics.get("target.bytes_replicated"), is(13L));
    assertThat(metrics.get("target.foo"), is(42L));
  }

  @Test
  public void typicalFailure() {
    listener.tableReplicationStart(tableReplication, "eventId");
    listener.copierStart("");
    listener.copierEnd(metrics);
    listener.tableReplicationFailure(tableReplication, "event-id", new RuntimeException());

    verify(scheduledReporter).start(2, TimeUnit.SECONDS);
    verify(scheduledReporter, times(2)).report();
    verify(scheduledReporter).stop();
    verify(metricSender).send(metricsCaptor.capture());

    Map<String, Long> metrics = metricsCaptor.getValue();

    assertThat(metrics.size(), is(3));
    assertThat(metrics.get("target.replication_time"), greaterThanOrEqualTo(0L));
    assertThat(metrics.get("target.completion_code"), is(-1L));
    assertThat(metrics.get("target.bytes_replicated"), is(0L));
  }

  @Test
  public void failureWithoutStartOrSuccessDoesntThrowException() {
    Throwable throwable = new Throwable("Test");
    listener.tableReplicationFailure(tableReplication, "event-id", throwable);
    verify(metricSender).send(metricsCaptor.capture());

    Map<String, Long> metrics = metricsCaptor.getValue();

    assertThat(metrics.size(), is(3));
    assertThat(metrics.get("target.replication_time"), is(-1L));
    assertThat(metrics.get("target.completion_code"), is(-1L));
    assertThat(metrics.get("target.bytes_replicated"), is(0L));
  }

}
