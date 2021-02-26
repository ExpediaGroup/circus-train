/**
 * Copyright (C) 2016-2021 Expedia, Inc.
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

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.hotels.bdp.circustrain.api.CircusTrainException;

@RunWith(MockitoJUnitRunner.class)
public class JobMetricsTest {

  private static final String GROUP = "group";
  private static final String COUNTER = "counter";

  @Mock
  private Job job;

  @Test
  public void nullJob() throws Exception {
    Map<String, Long> metrics = new JobMetrics(null, GROUP, COUNTER).getMetrics();

    assertThat(metrics.size(), is(0));
  }

  @Test
  public void nullCounters() throws Exception {
    when(job.getCounters()).thenReturn(null);

    Map<String, Long> metrics = new JobMetrics(job, GROUP, COUNTER).getMetrics();

    assertThat(metrics.size(), is(0));
  }

  @Test(expected = CircusTrainException.class)
  public void execptionGettingCounters() throws Exception {
    doThrow(IOException.class).when(job).getCounters();

    new JobMetrics(job, GROUP, COUNTER);
  }

  @Test
  public void typical() throws Exception {
    Counters counters = new Counters();
    counters.getGroup(GROUP).addCounter(COUNTER, COUNTER, 1L);

    when(job.getCounters()).thenReturn(counters);

    JobMetrics jobMetrics = new JobMetrics(job, GROUP, COUNTER);
    Map<String, Long> metrics = jobMetrics.getMetrics();

    assertThat(metrics.size(), is(1));
    assertThat(metrics.get("group.counter"), is(1L));
    assertThat(jobMetrics.getBytesReplicated(), is(1L));
  }

}
