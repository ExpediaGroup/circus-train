/**
 * Copyright (C) 2016-2017 Expedia Inc and Apache Hadoop contributors.
 *
 * Based on {@code org.apache.hadoop.tools.util.DistCpUtils} from Hadoop DistCp 2.7.1:
 *
 * https://github.com/apache/hadoop/blob/release-2.7.1/hadoop-tools/hadoop-distcp/src/main/java/org/
 * apache/hadoop/tools/util/DistCpUtils.java
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

import java.util.Locale;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;

import com.hotels.bdp.circustrain.s3mapreducecp.S3MapReduceCpOptions;
import com.hotels.bdp.circustrain.s3mapreducecp.mapreduce.UniformSizeInputFormat;

/**
 * Utility functions used in S3MapReduceCp.
 */
public final class ConfigurationUtil {

  private ConfigurationUtil() {}

  /**
   * Utility to publish a value to a configuration.
   *
   * @param configuration The Configuration to which the value must be written.
   * @param label The label for the value being published.
   * @param value The value being published.
   * @param <T> The type of the value.
   */
  public static <T> void publish(Configuration configuration, String label, T value) {
    configuration.set(label, String.valueOf(value));
  }

  /**
   * Utility to retrieve a specified key from a Configuration. Throws an exception if not found.
   *
   * @param configuration The Configuration in which the key is sought.
   * @param label The key being sought.
   * @return Integer value of the key.
   */
  public static int getInt(Configuration configuration, String label) {
    int value = configuration.getInt(label, -1);
    assert value >= 0 : "Couldn't find " + label;
    return value;
  }

  /**
   * Utility to retrieve a specified key from a Configuration. Throws an exception if not found.
   *
   * @param configuration The Configuration in which the key is sought.
   * @param label The key being sought.
   * @return Long value of the key.
   */
  public static long getLong(Configuration configuration, String label) {
    long value = configuration.getLong(label, -1);
    assert value >= 0 : "Couldn't find " + label;
    return value;
  }

  /**
   * Returns the class that implements a copy strategy. Looks up the implementation for a particular strategy from
   * s3mapreducecp-default.xml
   *
   * @param conf - Configuration object
   * @param options - Handle to input options
   * @return Class implementing the strategy specified in options.
   */
  public static Class<? extends InputFormat> getStrategy(Configuration conf, S3MapReduceCpOptions options) {
    String confLabel = "com.hotels.bdp.circustrain.s3mapreducecp."
        + options.getCopyStrategy().toLowerCase(Locale.getDefault())
        + ".strategy.impl";
    return conf.getClass(confLabel, UniformSizeInputFormat.class, InputFormat.class);
  }

}
