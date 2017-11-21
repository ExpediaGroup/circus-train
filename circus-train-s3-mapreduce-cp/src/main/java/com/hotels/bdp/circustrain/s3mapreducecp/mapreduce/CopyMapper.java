/**
 * Copyright (C) 2016-2017 Expedia Inc and Apache Hadoop contributors.
 *
 * Based on {@code org.apache.hadoop.tools.mapred.CopyMapper} from Hadoop DistCp 2.7.1:
 *
 * https://github.com/apache/hadoop/blob/release-2.7.1/hadoop-tools/hadoop-distcp/src/main/java/org/apache/hadoop/tools/mapred/CopyMapper.java
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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.client.builder.ExecutorFactory;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;

import com.hotels.bdp.circustrain.s3mapreducecp.ConfigurationVariable;
import com.hotels.bdp.circustrain.s3mapreducecp.CopyListingFileStatus;
import com.hotels.bdp.circustrain.s3mapreducecp.S3MapReduceCpConfiguration;
import com.hotels.bdp.circustrain.s3mapreducecp.S3MapReduceCpConstants;
import com.hotels.bdp.circustrain.s3mapreducecp.aws.AwsS3ClientFactory;
import com.hotels.bdp.circustrain.s3mapreducecp.util.PathUtil;

/**
 * Mapper class that executes the S3MapReduceCp copy operation. Implements the o.a.h.mapreduce.Mapper<> interface.
 */
public class CopyMapper extends Mapper<Text, CopyListingFileStatus, Text, Text> {
  private static final Logger LOG = LoggerFactory.getLogger(CopyMapper.class);

  private S3MapReduceCpConfiguration conf;

  private boolean ignoreFailures = false;
  private Path targetFinalPath;
  private TransferManager transferManager;

  /**
   * Implementation of the Mapper::setup() method. This extracts the S3MapReduceCp options specified in the Job's
   * configuration, to set up the Job.
   *
   * @param context Mapper's context.
   * @throws IOException On IO failure.
   * @throws InterruptedException If the job is interrupted.
   */
  @Override
  public void setup(Context context) throws IOException, InterruptedException {
    conf = new S3MapReduceCpConfiguration(context.getConfiguration());

    ignoreFailures = conf.getBoolean(ConfigurationVariable.IGNORE_FAILURES);

    targetFinalPath = new Path(conf.get(S3MapReduceCpConstants.CONF_LABEL_TARGET_FINAL_PATH));

    AwsS3ClientFactory awsS3ClientFactory = new AwsS3ClientFactory();
    transferManager = TransferManagerBuilder
        .standard()
        .withMinimumUploadPartSize(conf.getLong(ConfigurationVariable.MINIMUM_UPLOAD_PART_SIZE))
        .withMultipartUploadThreshold(conf.getLong(ConfigurationVariable.MULTIPART_UPLOAD_THRESHOLD))
        .withS3Client(awsS3ClientFactory.newInstance(conf))
        .withShutDownThreadPools(true)
        .withExecutorFactory(new ExecutorFactory() {
          @Override
          public ExecutorService newExecutor() {
            return Executors.newFixedThreadPool(conf.getInt(ConfigurationVariable.NUMBER_OF_UPLOAD_WORKERS));
          }
        })
        .build();
  }

  /**
   * Shutdown transfer queue and release other engaged resources.
   */
  @Override
  protected void cleanup(Mapper<Text, CopyListingFileStatus, Text, Text>.Context context)
    throws IOException, InterruptedException {
    if (transferManager != null) {
      transferManager.shutdownNow(true);
    }
  }

  /**
   * Implementation of the Mapper<>::map(). Does the copy.
   *
   * @param relPath The target path.
   * @param sourceFileStatus The source path.
   * @throws IOException
   */
  @Override
  public void map(Text relPath, CopyListingFileStatus sourceFileStatus, Context context)
    throws IOException, InterruptedException {
    Path sourcePath = sourceFileStatus.getPath();

    LOG.debug("S3MapReduceCp::map(): Received {}, {}", sourcePath, relPath);

    Path targetPath = new Path(targetFinalPath.toString() + relPath.toString());

    final String description = "Copying " + sourcePath + " to " + targetPath;
    context.setStatus(description);

    LOG.info(description);

    try {
      CopyListingFileStatus sourceCurrStatus;
      FileSystem sourceFS;
      try {
        sourceFS = sourcePath.getFileSystem(conf);
        sourceCurrStatus = new CopyListingFileStatus(sourceFS.getFileStatus(sourcePath));
      } catch (FileNotFoundException e) {
        throw new IOException(new RetriableFileCopyCommand.CopyReadException(e));
      }

      if (sourceCurrStatus.isDirectory()) {
        throw new RuntimeException("Copy listing must not contain directories. Found: " + sourceCurrStatus.getPath());
      }

      S3UploadDescriptor uploadDescriptor = describeUpload(sourceCurrStatus, targetPath);

      incrementCounter(context, Counter.BYTESEXPECTED, sourceFileStatus.getLen());
      long bytesCopied = copyFileWithRetry(description, context, sourceCurrStatus, uploadDescriptor);
      incrementCounter(context, Counter.BYTESCOPIED, bytesCopied);
      incrementCounter(context, Counter.COPY, 1L);

    } catch (IOException exception) {
      handleFailures(exception, sourceFileStatus, targetPath, context);
    }
  }

  private S3UploadDescriptor describeUpload(FileStatus sourceFileStatus, Path targetPath) throws IOException {
    URI targetUri = targetPath.toUri();
    String bucketName = PathUtil.toBucketName(targetUri);
    String key = PathUtil.toBucketKey(targetUri);

    Path sourcePath = sourceFileStatus.getPath();

    ObjectMetadata metadata = new ObjectMetadata();
    metadata.setContentLength(sourceFileStatus.getLen());
    if (conf.getBoolean(ConfigurationVariable.S3_SERVER_SIDE_ENCRYPTION)) {
      metadata.setSSEAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);
    }
    return new S3UploadDescriptor(sourcePath, bucketName, key, metadata);
  }

  private long copyFileWithRetry(
      String description,
      Context context,
      FileStatus sourceFileStatus,
      S3UploadDescriptor uploadDescriptor)
    throws IOException {
    try {
      return new RetriableFileCopyCommand(description, transferManager).execute(context, sourceFileStatus,
          uploadDescriptor);
    } catch (Exception e) {
      context.setStatus("Copy Failure: " + sourceFileStatus.getPath());
      throw new IOException("File copy failed: " + sourceFileStatus.getPath(), e);
    }
  }

  private void handleFailures(IOException exception, FileStatus sourceFileStatus, Path target, Context context)
    throws IOException, InterruptedException {
    LOG.error("Failure in copying {} to {}", sourceFileStatus.getPath(), target, exception);

    if (ignoreFailures && exception.getCause() instanceof RetriableFileCopyCommand.CopyReadException) {
      incrementCounter(context, Counter.FAIL, 1);
      incrementCounter(context, Counter.BYTESFAILED, sourceFileStatus.getLen());
      context.write(null,
          new Text("FAIL: " + sourceFileStatus.getPath() + " - " + StringUtils.stringifyException(exception)));
    } else {
      throw exception;
    }
  }

  /**
   * Increments the given counter by the given value.
   *
   * @param context Mapper context
   * @param counter Hadoop counter
   * @param value Increment
   */
  private static void incrementCounter(Context context, Counter counter, long value) {
    context.getCounter(counter).increment(value);
  }

}
