/**
 * Copyright (C) 2016-2017 Expedia Inc and Apache Hadoop contributors.
 *
 * Based on {@code org.apache.hadoop.tools.util.TestDistCpUtils} from Hadoop DistCp 2.7.1:
 *
 * https://github.com/apache/hadoop/blob/release-2.7.1/hadoop-tools/hadoop-distcp/src/test/java/org/apache/hadoop/tools/util/TestDistCpUtils.java
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
package com.hotels.bdp.circustrain.s3mapreducecp.util;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.hamcrest.CoreMatchers;
import org.junit.Test;

import com.hotels.bdp.circustrain.s3mapreducecp.CopyListingFileStatus;
import com.hotels.bdp.circustrain.s3mapreducecp.S3MapReduceCpOptions;
import com.hotels.bdp.circustrain.s3mapreducecp.mapreduce.UniformSizeInputFormat;

public class ConfigurationUtilTest {

  private Configuration config = new Configuration();

  @Test
  public void publish() {
    class T {
      @Override
      public String toString() {
        return "Hello world!";
      }
    }
    ConfigurationUtil.publish(config, "a.b.c", new T());
    assertThat(config.get("a.b.c"), is("Hello world!"));
  }

  @Test(expected = AssertionError.class)
  public void getUnknownIntProperty() {
    ConfigurationUtil.getInt(config, "a.b.c");
  }

  @Test
  public void getIntProperty() {
    config.set("a.b.c", "1024");
    assertThat(ConfigurationUtil.getInt(config, "a.b.c"), is(1024));
  }

  @Test(expected = AssertionError.class)
  public void getUnknownLongProperty() {
    ConfigurationUtil.getLong(config, "a.b.c");
  }

  @Test
  public void getLongProperty() {
    config.set("a.b.c", "1234567890");
    assertThat(ConfigurationUtil.getLong(config, "a.b.c"), is(1234567890L));
  }

  @Test
  public void getDefaultStrategy() {
    S3MapReduceCpOptions options = new S3MapReduceCpOptions();
    assertThat(ConfigurationUtil.getStrategy(config, options),
        is(CoreMatchers.<Class<?>> equalTo(UniformSizeInputFormat.class)));
  }

  @Test
  public void getCustomStrategy() {
    class CustomInputFormat extends InputFormat<Text, CopyListingFileStatus> {
      @Override
      public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
        return null;
      }

      @Override
      public RecordReader<Text, CopyListingFileStatus> createRecordReader(InputSplit split, TaskAttemptContext context)
        throws IOException, InterruptedException {
        return null;
      }
    }
    config.set("com.hotels.bdp.circustrain.s3mapreducecp.my-strategy.strategy.impl", CustomInputFormat.class.getName());
    S3MapReduceCpOptions options = S3MapReduceCpOptions.builder(null, null).copyStrategy("my-strategy").build();
    assertThat(ConfigurationUtil.getStrategy(config, options),
        is(CoreMatchers.<Class<?>> equalTo(CustomInputFormat.class)));
  }

}
