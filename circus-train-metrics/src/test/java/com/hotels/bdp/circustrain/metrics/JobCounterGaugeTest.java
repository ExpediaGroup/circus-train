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
import static org.mockito.Mockito.when;

import java.io.IOException;

import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class JobCounterGaugeTest {

  private enum TestEnum {
    TEST;
  }

  private @Mock Job job;
  private @Mock Counters counters;
  private @Mock Counter counter;

  private final String counterName = "someName";
  private final String groupName = "groupName";
  private final Enum<?> key = TestEnum.TEST;
  private final Long expectedResult = 10L;
  private JobCounterGauge jobCounterGauge;

  @Before
  public void setUp() throws IOException {
    when(job.getCounters()).thenReturn(counters);
    when(counters.findCounter(key)).thenReturn(counter);
    when(counters.findCounter(groupName, counterName)).thenReturn(counter);
    when(counter.getValue()).thenReturn(expectedResult);
  }

  @Test
  public void getValue() {
    jobCounterGauge = new JobCounterGauge(job, key);
    Long result = jobCounterGauge.getValue();
    assertThat(result, is(expectedResult));
  }

  @Test
  public void getValueGroupName() {
    jobCounterGauge = new JobCounterGauge(job, groupName, counterName);
    Long result = jobCounterGauge.getValue();
    assertThat(result, is(expectedResult));
  }

  @Test
  public void getValueCannotGetCounters() throws IOException {
    when(job.getCounters()).thenThrow(new IOException());
    jobCounterGauge = new JobCounterGauge(job, key);
    Long result = jobCounterGauge.getValue();
    assertThat(result, is(0L));
  }
}
