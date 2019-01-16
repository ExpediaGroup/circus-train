/**
 * Copyright (C) 2016-2017 Expedia Inc and Apache Hadoop contributors.
 *
 * Based on {@code org.apache.hadoop.tools.mapred.CopyMapper.Counter} from Hadoop DistCp 2.7.1:
 *
 * https://github.com/apache/hadoop/blob/release-2.7.1/hadoop-tools/hadoop-distcp/src/main/java/org/
 * apache/hadoop/tools/mapred/CopyMapper.java#L57
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
package com.hotels.bdp.circustrain.s3mapreducecp.mapreduce;

/**
 * Hadoop counters for the S3MapReduceCp CopyMapper. (These have been kept identical to the old S3MapReduceCp, for
 * backward compatibility.)
 */
public enum Counter {
  COPY, // Number of files received by the mapper for copy.
  SKIP, // Number of files skipped.
  FAIL, // Number of files that failed to be copied.
  BYTESCOPIED, // Number of bytes actually copied by the copy-mapper, total.
  BYTESEXPECTED, // Number of bytes expected to be copied.
  BYTESFAILED, // Number of bytes that failed to be copied.
  BYTESSKIPPED, // Number of bytes that were skipped from copy.
}
