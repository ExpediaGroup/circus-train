/**
 * Copyright (C) 2016-2017 Expedia Inc and Apache Hadoop contributors.
 *
 * Based on {@code org.apache.hadoop.tools.util.DistCpUtils} from Hadoop DistCp 2.7.1:
 *
 * https://github.com/apache/hadoop/blob/release-2.7.1/hadoop-tools/hadoop-distcp/src/main/java/org/apache/hadoop/tools/util/DistCpUtils.java
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
package com.hotels.bdp.circustrain.s3mapreducecp.io;

import java.text.DecimalFormat;

/**
 * Utility functions used in S3MapReduceCp.
 */
public class BytesFormatter {

  /**
   * String utility to convert a number-of-bytes to human readable format.
   */
  private static ThreadLocal<DecimalFormat> FORMATTER = new ThreadLocal<DecimalFormat>() {
    @Override
    protected DecimalFormat initialValue() {
      return new DecimalFormat("0.0");
    }
  };

  public static DecimalFormat getFormatter() {
    return FORMATTER.get();
  }

  public static String getStringDescriptionFor(long nBytes) {

    char units[] = { 'B', 'K', 'M', 'G', 'T', 'P' };

    double current = nBytes;
    double prev = current;
    int index = 0;

    while ((current = current / 1024) >= 1) {
      prev = current;
      ++index;
    }

    assert index < units.length : "Too large a number.";

    return getFormatter().format(prev) + units[index];
  }

}
