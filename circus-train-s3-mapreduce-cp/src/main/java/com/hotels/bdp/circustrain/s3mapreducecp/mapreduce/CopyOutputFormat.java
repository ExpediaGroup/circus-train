/**
 * Copyright (C) 2016-2017 Expedia Inc and Apache Hadoop contributors.
 *
 * Based on {@code org.apache.hadoop.tools.mapred.CopyOutputFormat} from Hadoop DistCp 2.7.1:
 *
 * https://github.com/apache/hadoop/blob/release-2.7.1/hadoop-tools/hadoop-distcp/src/main/java/org/apache/hadoop/tools/mapred/CopyOutputFormat.java
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

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.security.TokenCache;

import com.hotels.bdp.circustrain.s3mapreducecp.S3MapReduceCpConstants;

/**
 * The CopyOutputFormat is the Hadoop OutputFormat used in S3MapReduceCp. It sets up the Job's Configuration (in the
 * Job-Context) with the settings for the final commit-directory, etc. It also sets the right output-committer.
 *
 * @param <K>
 * @param <V>
 */
public class CopyOutputFormat<K, V> extends TextOutputFormat<K, V> {

  /**
   * Setter for the final directory for S3MapReduceCp (where files copied will be moved, atomically.)
   *
   * @param job The Job on whose configuration the working-directory is to be set.
   * @param commitDirectory The path to use for final commit.
   */
  public static void setCommitDirectory(Job job, Path commitDirectory) {
    job.getConfiguration().set(S3MapReduceCpConstants.CONF_LABEL_TARGET_FINAL_PATH, commitDirectory.toString());
  }

  /**
   * Getter for the final commit-directory.
   *
   * @param job The Job from whose configuration the commit-directory is to be retrieved.
   * @return The commit-directory Path.
   */
  public static Path getCommitDirectory(Job job) {
    return getCommitDirectory(job.getConfiguration());
  }

  private static Path getCommitDirectory(Configuration conf) {
    String commitDirectory = conf.get(S3MapReduceCpConstants.CONF_LABEL_TARGET_FINAL_PATH);
    if (commitDirectory == null || commitDirectory.isEmpty()) {
      return null;
    } else {
      return new Path(commitDirectory);
    }
  }

  /** @inheritDoc */
  @Override
  public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException {
    return new CopyCommitter(getOutputPath(context), context);
  }

  /** @inheritDoc */
  @Override
  public void checkOutputSpecs(JobContext context) throws IOException {
    Configuration conf = context.getConfiguration();

    Path workingPath = getCommitDirectory(conf);
    if (getCommitDirectory(conf) == null) {
      throw new IllegalStateException("Commit directory not configured");
    }

    // get delegation token for outDir's file system
    TokenCache.obtainTokensForNamenodes(context.getCredentials(), new Path[] { workingPath }, conf);
  }
}
