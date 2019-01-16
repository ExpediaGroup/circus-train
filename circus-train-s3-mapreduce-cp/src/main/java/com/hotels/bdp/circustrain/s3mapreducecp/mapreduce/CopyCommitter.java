/**
 * Copyright (C) 2016-2017 Expedia Inc and Apache Hadoop contributors.
 *
 * Based on {@code org.apache.hadoop.tools.mapred.CopyCommitter} from Hadoop DistCp 2.7.1:
 *
 * https://github.com/apache/hadoop/blob/release-2.7.1/hadoop-tools/hadoop-distcp/src/main/java/org/
 * apache/hadoop/tools/mapred/CopyCommitter.java
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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hotels.bdp.circustrain.s3mapreducecp.S3MapReduceCpConstants;

/**
 * The CopyCommitter class is S3MapReduceCp's OutputCommitter implementation. It is responsible for handling the
 * completion/cleanup of the S3MapReduceCp run. Specifically, it does cleanup of the meta-folder (where S3MapReduceCp
 * maintains its file-list, etc.)
 */
public class CopyCommitter extends FileOutputCommitter {
  private static final Logger LOG = LoggerFactory.getLogger(CopyCommitter.class);

  private final TaskAttemptContext taskAttemptContext;

  /**
   * Create a output committer
   *
   * @param outputPath the job's output path
   * @param context the task's context
   * @throws IOException - Exception if any
   */
  public CopyCommitter(Path outputPath, TaskAttemptContext context) throws IOException {
    super(outputPath, context);
    taskAttemptContext = context;
  }

  /** @inheritDoc */
  @Override
  public void commitJob(JobContext jobContext) throws IOException {
    Configuration conf = jobContext.getConfiguration();

    super.commitJob(jobContext);

    try {
      taskAttemptContext.setStatus("Commit Successful");
    } finally {
      cleanup(conf);
    }
  }

  /** @inheritDoc */
  @Override
  public void abortJob(JobContext jobContext, JobStatus.State state) throws IOException {
    try {
      super.abortJob(jobContext, state);
    } finally {
      cleanup(jobContext.getConfiguration());
    }
  }

  /**
   * Cleanup meta folder and other temporary files
   *
   * @param conf - Job Configuration
   */
  private void cleanup(Configuration conf) {
    Path metaFolder = new Path(conf.get(S3MapReduceCpConstants.CONF_LABEL_META_FOLDER));
    try {
      FileSystem fs = metaFolder.getFileSystem(conf);
      LOG.info("Cleaning up temporary work folder: {}", metaFolder);
      fs.delete(metaFolder, true);
    } catch (IOException ignore) {
      LOG.error("Exception encountered ", ignore);
    }
  }

}
