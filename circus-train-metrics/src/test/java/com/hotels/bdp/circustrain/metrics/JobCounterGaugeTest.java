package com.hotels.bdp.circustrain.metrics;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

import java.io.IOException;

import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

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
