/**
 * Copyright (C) 2016-2019 Expedia, Inc.
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
package com.hotels.bdp.circustrain.metrics.conf;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.codahale.metrics.MetricRegistry;

import com.hotels.bdp.circustrain.api.metrics.LoggingScheduledReporterFactory;
import com.hotels.bdp.circustrain.api.metrics.MetricSender;
import com.hotels.bdp.circustrain.api.metrics.ScheduledReporterFactory;
import com.hotels.bdp.circustrain.metrics.GraphiteMetricSender;
import com.hotels.bdp.circustrain.metrics.GraphiteScheduledReporterFactory;

@RunWith(MockitoJUnitRunner.class)
public class MetricsConfTest {

  private @Mock ValidatedGraphite validatedGraphite;

  @Test
  public void graphiteMetricSender() {
    when(validatedGraphite.getHost()).thenReturn("localhost:123");
    when(validatedGraphite.getFormattedPrefix()).thenReturn("prefix.namespace");
    when(validatedGraphite.isEnabled()).thenReturn(true);
    MetricSender sender = new MetricsConf().metricSender(validatedGraphite);

    assertTrue(sender instanceof GraphiteMetricSender);
  }

  @Test
  public void defaultMetricSender() {
    when(validatedGraphite.isEnabled()).thenReturn(false);
    MetricSender sender = new MetricsConf().metricSender(validatedGraphite);

    assertTrue(sender == MetricSender.DEFAULT_LOG_ONLY);
  }

  @Test
  public void graphiteReporter() {
    when(validatedGraphite.isEnabled()).thenReturn(true);
    ScheduledReporterFactory reporterFactory = new MetricsConf().runningScheduledReporterFactory(new MetricRegistry(),
        validatedGraphite);

    assertTrue(reporterFactory instanceof GraphiteScheduledReporterFactory);
  }

  @Test
  public void defaultReporter() {
    when(validatedGraphite.isEnabled()).thenReturn(false);
    ScheduledReporterFactory reporterFactory = new MetricsConf().runningScheduledReporterFactory(new MetricRegistry(),
        validatedGraphite);

    assertTrue(reporterFactory instanceof LoggingScheduledReporterFactory);
  }

}
