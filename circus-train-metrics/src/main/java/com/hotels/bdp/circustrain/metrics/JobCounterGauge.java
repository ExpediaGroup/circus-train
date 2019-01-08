package com.hotels.bdp.circustrain.metrics;

import java.io.IOException;

import org.apache.hadoop.mapreduce.FileSystemCounter;
import org.apache.hadoop.mapreduce.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Gauge;

public class JobCounterGauge implements Gauge<Long> {

  private static final Logger LOG = LoggerFactory.getLogger(JobCounterGauge.class);

  private final Job job;
  private final Enum<?> counter;
  private final String groupName;

  public JobCounterGauge(Job job, String groupName) {
    this(job, null, groupName);
  }

  public JobCounterGauge(Job job, Enum<?> counter) {
    this(job, counter, null);
  }

  public JobCounterGauge(Job job, Enum<?> counter, String groupName) {
    this.job = job;
    this.counter = counter;
    this.groupName = groupName;
  }

  @Override
  public Long getValue() {
    try {
      if (groupName != null) {
        return job.getCounters().findCounter(FileSystemCounter.class.getName(), groupName).getValue();
      } else {
        return job.getCounters().findCounter(counter).getValue();
      }
    } catch (IOException e) {
      LOG.warn("Could not get value for counter " + counter, e);
    }
    return 0L;
  }
}
