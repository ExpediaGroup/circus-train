/**
 * Copyright (C) 2016-2017 Expedia Inc and Apache Hadoop contributors.
 *
 * Based on {@code org.apache.hadoop.tools.StubContext} from Hadoop DistCp 2.7.1:
 *
 * https://github.com/apache/hadoop/blob/release-2.7.1/hadoop-tools/hadoop-distcp/src/test/java/org/apache/hadoop/tools/StubContext.java
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
package com.hotels.bdp.circustrain.s3mapreducecp;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.StatusReporter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.lib.map.WrappedMapper;
import org.apache.hadoop.mapreduce.task.MapContextImpl;

public class StubContext {

  private StubStatusReporter reporter = new StubStatusReporter();
  private RecordReader<Text, CopyListingFileStatus> reader;
  private StubInMemoryWriter writer = new StubInMemoryWriter();
  private Mapper<Text, CopyListingFileStatus, Text, Text>.Context mapperContext;

  public StubContext(Configuration conf, RecordReader<Text, CopyListingFileStatus> reader, int taskId)
      throws IOException, InterruptedException {

    WrappedMapper<Text, CopyListingFileStatus, Text, Text> wrappedMapper = new WrappedMapper<>();

    MapContextImpl<Text, CopyListingFileStatus, Text, Text> contextImpl = new MapContextImpl<>(conf,
        getTaskAttemptID(taskId), reader, writer, null, reporter, null);

    this.reader = reader;
    mapperContext = wrappedMapper.getMapContext(contextImpl);
  }

  public Mapper<Text, CopyListingFileStatus, Text, Text>.Context getContext() {
    return mapperContext;
  }

  public StatusReporter getReporter() {
    return reporter;
  }

  public RecordReader<Text, CopyListingFileStatus> getReader() {
    return reader;
  }

  public StubInMemoryWriter getWriter() {
    return writer;
  }

  public static class StubStatusReporter extends StatusReporter {

    private Counters counters = new Counters();

    public StubStatusReporter() {
      /*
       * final CounterGroup counterGroup = new CounterGroup("FileInputFormatCounters", "FileInputFormatCounters");
       * counterGroup.addCounter(new Counter("BYTES_READ", "BYTES_READ", 0)); counters.addGroup(counterGroup);
       */
    }

    @Override
    public Counter getCounter(Enum<?> name) {
      return counters.findCounter(name);
    }

    @Override
    public Counter getCounter(String group, String name) {
      return counters.findCounter(group, name);
    }

    @Override
    public void progress() {}

    @Override
    public float getProgress() {
      return 0F;
    }

    @Override
    public void setStatus(String status) {}
  }

  public static class StubInMemoryWriter extends RecordWriter<Text, Text> {

    List<Text> keys = new ArrayList<>();

    List<Text> values = new ArrayList<>();

    @Override
    public void write(Text key, Text value) throws IOException, InterruptedException {
      keys.add(key);
      values.add(value);
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {}

    public List<Text> keys() {
      return keys;
    }

    public List<Text> values() {
      return values;
    }

  }

  public static TaskAttemptID getTaskAttemptID(int taskId) {
    return new TaskAttemptID("", 0, TaskType.MAP, taskId, 0);
  }
}
