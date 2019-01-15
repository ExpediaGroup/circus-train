/**
 * Copyright (C) 2016-2019 Expedia Inc and Apache Hadoop contributors.
 *
 * Based on {@code org.apache.hadoop.tools.mapred.RetriableFileCopyCommand} from Hadoop DistCp 2.7.1:
 *
 * https://github.com/apache/hadoop/blob/release-2.7.1/hadoop-tools/hadoop-distcp/src/main/java/org/
 * apache/hadoop/tools/mapred/RetriableFileCopyCommand.java
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

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_DEFAULT;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_KEY;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.AmazonClientException;
import com.amazonaws.event.ProgressEvent;
import com.amazonaws.event.ProgressListener;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.transfer.Transfer;
import com.amazonaws.services.s3.transfer.TransferManager;

import com.hotels.bdp.circustrain.aws.CannedAclUtils;
import com.hotels.bdp.circustrain.s3mapreducecp.ConfigurationVariable;
import com.hotels.bdp.circustrain.s3mapreducecp.command.RetriableCommand;
import com.hotels.bdp.circustrain.s3mapreducecp.io.ThrottledInputStream;

/**
 * This class extends RetriableCommand to implement the copy of files, with retries on failure.
 */
public class RetriableFileCopyCommand extends RetriableCommand<Long> {
  private static final Logger LOG = LoggerFactory.getLogger(RetriableFileCopyCommand.class);

  private final TransferManager transferManager;

  private static class UploadProgressListener implements ProgressListener {
    private final Mapper.Context context;
    private final String description;

    UploadProgressListener(Mapper.Context context, String description) {
      this.context = context;
      this.description = description;
    }

    @Override
    public void progressChanged(ProgressEvent progressEvent) {
      StringBuilder message = new StringBuilder();
      switch (progressEvent.getEventType()) {
      case TRANSFER_STARTED_EVENT:
        message.append("Starting: ").append(description);
        break;
      case TRANSFER_COMPLETED_EVENT:
        message.append("Completed: ").append(description);
        break;
      case TRANSFER_FAILED_EVENT:
        message.append("Falied: ").append(description);
        break;
      default:
        break;
      }

      context.setStatus(message.toString());
    }
  }

  /**
   * Constructor, taking a description of the action and a {@code TransferManager}.
   *
   * @param description Verbose description of the copy operation.
   * @param transferManager AWS S3 transfer manager
   */
  public RetriableFileCopyCommand(String description, TransferManager transferManager) {
    super(description);
    this.transferManager = transferManager;
  }

  /**
   * Implementation of RetriableCommand::doExecute(). This is the actual copy-implementation.
   *
   * @param arguments Argument-list to the command.
   * @return Number of bytes copied.
   * @throws Exception: CopyReadException, if there are read-failures. All other failures are IOExceptions.
   */
  @Override
  protected Long doExecute(Object... arguments) throws Exception {
    assert arguments.length == 3 : "Unexpected argument list.";
    Mapper.Context context = (Mapper.Context) arguments[0];
    FileStatus source = (FileStatus) arguments[1];
    assert !source.isDirectory() : "Unexpected file-status. Expected file.";
    S3UploadDescriptor uploadDescriptor = (S3UploadDescriptor) arguments[2];
    return doCopy(context, source, uploadDescriptor);
  }

  private long doCopy(Mapper.Context context, FileStatus sourceFileStatus, S3UploadDescriptor uploadDescriptor)
    throws IOException {
    LOG.debug("Copying {} to {}", sourceFileStatus.getPath(), uploadDescriptor.getTargetPath());

    final Path sourcePath = sourceFileStatus.getPath();

    Transfer transfer = startTransfer(context, uploadDescriptor);
    transfer.addProgressListener(new UploadProgressListener(context, description));
    try {
      AmazonClientException e = transfer.waitForException();
      if (e != null) {
        throw new IOException(e);
      }
    } catch (InterruptedException e) {
      throw new RuntimeException("Unable to upload file " + sourcePath, e);
    }

    return transfer.getProgress().getBytesTransferred();
  }

  private static ThrottledInputStream getInputStream(Path path, Configuration conf) throws IOException {
    try {
      FileSystem fs = path.getFileSystem(conf);
      long bandwidthMB = conf
          .getInt(ConfigurationVariable.MAX_BANDWIDTH.getName(), ConfigurationVariable.MAX_BANDWIDTH.defaultIntValue());
      FSDataInputStream in = fs.open(path);
      return new ThrottledInputStream(in, bandwidthMB * 1024 * 1024);
    } catch (IOException e) {
      throw new CopyReadException(e);
    }
  }

  private Transfer startTransfer(Mapper.Context context, S3UploadDescriptor uploadDescriptor) throws IOException {
    InputStream input = getInputStream(uploadDescriptor.getSource(), context.getConfiguration());
    int bufferSize = context.getConfiguration().getInt(ConfigurationVariable.UPLOAD_BUFFER_SIZE.getName(), -1);
    if (bufferSize <= 0) {
      // The default value is the same value used by FileSystem to configure the InputStream.
      // See https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/core-default.xml
      bufferSize = context.getConfiguration().getInt(IO_FILE_BUFFER_SIZE_KEY, IO_FILE_BUFFER_SIZE_DEFAULT);
    }
    LOG.info("Buffer of the input stream is {} for file {}", bufferSize, uploadDescriptor.getSource());

    try (BufferedInputStream bufferedInputStream = new BufferedInputStream(input, bufferSize)) {
      PutObjectRequest request = new PutObjectRequest(uploadDescriptor.getBucketName(), uploadDescriptor.getKey(),
          bufferedInputStream, uploadDescriptor.getMetadata());

      String cannedAcl = context.getConfiguration().get(ConfigurationVariable.CANNED_ACL.getName());
      if (cannedAcl != null) {
        CannedAccessControlList acl = CannedAclUtils.toCannedAccessControlList(cannedAcl);
        LOG.debug("Using CannedACL {}", acl.name());
        request.withCannedAcl(acl);
      }

      // We add 1 to the buffer size as per the com.amazonaws.RequestClientOptions doc
      request.getRequestClientOptions().setReadLimit(bufferSize + 1);
      return transferManager.upload(request);
    } catch (AmazonClientException | IOException e) {
      throw new CopyReadException(e);
    }
  }

  /**
   * Special subclass of IOException. This is used to distinguish read-operation failures from other kinds of
   * IOExceptions. The failure to read from source is dealt with specially in the CopyMapper. Such failures may be
   * skipped if the S3MapReduceCpOptions indicate so. Write failures are intolerable and amount to CopyMapper failure.
   */
  public static class CopyReadException extends IOException {
    private static final long serialVersionUID = 1L;

    public CopyReadException(Throwable rootCause) {
      super(rootCause);
    }
  }

}
