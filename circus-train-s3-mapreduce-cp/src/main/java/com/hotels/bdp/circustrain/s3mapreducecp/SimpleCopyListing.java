/**
 * Copyright (C) 2016-2017 Expedia Inc and Apache Hadoop contributors.
 *
 * Based on {@code org.apache.hadoop.tools.SimpleCopyListing} and
 * {@code org.apache.hadoop.tools.GlobbedCopyListing} from Hadoop DistCp 2.7.1:
 *
 * https://github.com/apache/hadoop/blob/release-2.7.1/hadoop-tools/hadoop-distcp/src/main/java/org/
 * apache/hadoop/tools/SimpleCopyListing.java
 * https://github.com/apache/hadoop/blob/release-2.7.1/hadoop-tools/hadoop-distcp/src/main/java/org/
 * apache/hadoop/tools/GlobbedCopyListing.java
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
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.security.Credentials;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

import com.hotels.bdp.circustrain.s3mapreducecp.util.IoUtil;
import com.hotels.bdp.circustrain.s3mapreducecp.util.PathUtil;

/**
 * The SimpleCopyListing is responsible for making the exhaustive list of all files/directories under its specified list
 * of input-paths. These are written into the specified copy-listing file. Note: The SimpleCopyListing doesn't handle
 * wild-cards in the input-paths.
 */
public class SimpleCopyListing extends CopyListing {
  private static final Logger LOG = LoggerFactory.getLogger(SimpleCopyListing.class);

  public static final String CONF_LABEL_ROOT_PATH = "com.hotels.bdp.circustrain.s3mapreducecp.SimpleCopyListing.rootPath";

  private long totalPaths = 0;
  private long totalBytesToCopy = 0;
  private final Path rootPath;

  /**
   * Protected constructor, to initialize configuration.
   *
   * @param configuration The input configuration, with which the source/target FileSystems may be accessed.
   * @param credentials - Credentials object on which the FS delegation tokens are cached. If null delegation token
   *          caching is skipped
   */
  protected SimpleCopyListing(Configuration configuration, Credentials credentials) {
    super(configuration, credentials);
    Configuration conf = getConf();
    String rootPathString = conf.get(CONF_LABEL_ROOT_PATH);
    rootPath = (rootPathString != null && !rootPathString.isEmpty()) ? new Path(rootPathString) : null;
  }

  /** {@inheritDoc} */
  @Override
  protected void validatePath(S3MapReduceCpOptions options) throws IOException, InvalidInputException {
    List<Path> sourcePaths = options.getSources();
    for (Path sourcePath : sourcePaths) {
      FileSystem fs = sourcePath.getFileSystem(getConf());
      if (!fs.exists(sourcePath)) {
        throw new InvalidInputException("Source path " + sourcePath + " doesn't exist");
      }
    }

    URI target = options.getTarget();
    if (sourcePaths.size() > 1) {
      if (PathUtil.isFile(target)) {
        throw new InvalidInputException("Cannot copy source paths " + sourcePaths + " to file " + target);
      }
    } else {
      Path sourcePath = sourcePaths.get(0);
      FileSystem fs = sourcePath.getFileSystem(getConf());
      FileStatus fileStatus = fs.getFileStatus(sourcePath);
      if (fileStatus.isDirectory() && PathUtil.isFile(target)) {
        throw new InvalidInputException("Cannot copy source directory " + sourcePath + " to file " + target);
      }
    }

    /*
     * This is requires to allow map tasks to access each of the source clusters. This would retrieve the delegation
     * token for each unique file system and add them to job's private credential store
     */
    Credentials credentials = getCredentials();
    if (credentials != null) {
      Path[] inputPaths = options.getSources().toArray(new Path[options.getSources().size()]);
      TokenCache.obtainTokensForNamenodes(credentials, inputPaths, getConf());
    }
  }

  /** {@inheritDoc} */
  @Override
  public void doBuildListing(Path pathToListingFile, S3MapReduceCpOptions options) throws IOException {
    doBuildListing(getWriter(pathToListingFile), options);
  }

  /**
   * Collect the list of <sourceRelativePath, sourceFileStatus> to be copied and write to the sequence file. In essence,
   * any file or directory that need to be copied or sync-ed is written as an entry to the sequence file, with the
   * possible exception of the source root: when either -update (sync) or -overwrite switch is specified, and if the the
   * source root is a directory, then the source root entry is not written to the sequence file, because only the
   * contents of the source directory need to be copied in this case. See
   * {@link com.hotels.bdp.circustrain.s3mapreducecp.util.ConfigurationUtil#getRelativePath} for how relative path is
   * computed. See computeSourceRootPath method for how the root path of the source is computed.
   *
   * @param fileListWriter
   * @param options
   * @param globbedPaths
   * @throws IOException
   */
  @VisibleForTesting
  public void doBuildListing(SequenceFile.Writer fileListWriter, S3MapReduceCpOptions options) throws IOException {
    List<Path> globbedPaths = new ArrayList<>(options.getSources().size());

    for (Path sourcePath : options.getSources()) {
      FileSystem fs = sourcePath.getFileSystem(getConf());
      FileStatus sourceFileStatus = fs.getFileStatus(sourcePath);
      if (sourceFileStatus.isFile()) {
        LOG.debug("Adding path {}", sourceFileStatus.getPath());
        globbedPaths.add(sourceFileStatus.getPath());
      } else {
        FileStatus[] inputs = fs.globStatus(sourcePath);
        if (inputs != null && inputs.length > 0) {
          for (FileStatus onePath : inputs) {
            LOG.debug("Adding path {}", onePath.getPath());
            globbedPaths.add(onePath.getPath());
          }
        } else {
          throw new InvalidInputException("Source path " + sourcePath + " doesn't exist");
        }
      }
    }
    doBuildListing(fileListWriter, options, globbedPaths);
  }

  @VisibleForTesting
  public void doBuildListing(SequenceFile.Writer fileListWriter, S3MapReduceCpOptions options, List<Path> globbedPaths)
    throws IOException {
    try {
      for (Path path : globbedPaths) {
        FileSystem sourceFS = path.getFileSystem(getConf());
        path = makeQualified(path);

        FileStatus rootStatus = sourceFS.getFileStatus(path);
        Path sourcePathRoot = computeSourceRootPath(rootStatus, options);
        LOG.info("Root source path is {}", sourcePathRoot);

        FileStatus[] sourceFiles = sourceFS.listStatus(path);
        boolean explore = (sourceFiles != null && sourceFiles.length > 0);
        if (explore || rootStatus.isDirectory()) {
          for (FileStatus sourceStatus : sourceFiles) {
            if (sourceStatus.isFile()) {
              LOG.debug("Recording source-path: {} for copy.", sourceStatus.getPath());
              CopyListingFileStatus sourceCopyListingStatus = new CopyListingFileStatus(sourceStatus);
              writeToFileListing(fileListWriter, sourceCopyListingStatus, sourcePathRoot, options);
            }
            if (isDirectoryAndNotEmpty(sourceFS, sourceStatus)) {
              LOG.debug("Traversing non-empty source dir: {}", sourceStatus.getPath());
              traverseNonEmptyDirectory(fileListWriter, sourceStatus, sourcePathRoot, options);
            }
          }
        }
      }
      fileListWriter.close();
      fileListWriter = null;
    } finally {
      IoUtil.closeSilently(LOG, fileListWriter);
    }
  }

  protected Path computeSourceRootPath(FileStatus sourceStatus, S3MapReduceCpOptions options) throws IOException {
    if (rootPath != null) {
      return rootPath;
    }
    return sourceStatus.isDirectory() ? sourceStatus.getPath() : sourceStatus.getPath().getParent();
  }

  /**
   * Provide an option to skip copy of a path, Allows for exclusion of files such as
   * {@link org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter#SUCCEEDED_FILE_NAME}
   *
   * @param path - Path being considered for copy while building the file listing
   * @param options - Input options passed during S3MapReduceCp invocation
   * @return - True if the path should be considered for copy, false otherwise
   */
  protected boolean shouldCopy(Path path, S3MapReduceCpOptions options) {
    return !path.getName().equals(FileOutputCommitter.SUCCEEDED_FILE_NAME);
  }

  /** {@inheritDoc} */
  @Override
  protected long getBytesToCopy() {
    return totalBytesToCopy;
  }

  /** {@inheritDoc} */
  @Override
  protected long getNumberOfPaths() {
    return totalPaths;
  }

  private Path makeQualified(Path path) throws IOException {
    final FileSystem fs = path.getFileSystem(getConf());
    return path.makeQualified(fs.getUri(), fs.getWorkingDirectory());
  }

  private SequenceFile.Writer getWriter(Path pathToListFile) throws IOException {
    FileSystem fs = pathToListFile.getFileSystem(getConf());
    if (fs.exists(pathToListFile)) {
      fs.delete(pathToListFile, false);
    }
    return SequenceFile
        .createWriter(getConf(), SequenceFile.Writer.file(pathToListFile), SequenceFile.Writer.keyClass(Text.class),
            SequenceFile.Writer.valueClass(CopyListingFileStatus.class),
            SequenceFile.Writer.compression(SequenceFile.CompressionType.NONE));
  }

  private static boolean isDirectoryAndNotEmpty(FileSystem fileSystem, FileStatus fileStatus) throws IOException {
    return fileStatus.isDirectory() && getChildren(fileSystem, fileStatus).length > 0;
  }

  private static FileStatus[] getChildren(FileSystem fileSystem, FileStatus parent) throws IOException {
    return fileSystem.listStatus(parent.getPath());
  }

  private void traverseNonEmptyDirectory(
      SequenceFile.Writer fileListWriter,
      FileStatus sourceStatus,
      Path sourcePathRoot,
      S3MapReduceCpOptions options)
    throws IOException {
    FileSystem sourceFS = sourcePathRoot.getFileSystem(getConf());
    Stack<FileStatus> pathStack = new Stack<>();
    pathStack.push(sourceStatus);

    while (!pathStack.isEmpty()) {
      for (FileStatus child : getChildren(sourceFS, pathStack.pop())) {
        if (child.isFile()) {
          LOG.debug("Recording source-path: {} for copy.", sourceStatus.getPath());
          CopyListingFileStatus childCopyListingStatus = new CopyListingFileStatus(child);
          writeToFileListing(fileListWriter, childCopyListingStatus, sourcePathRoot, options);
        }
        if (isDirectoryAndNotEmpty(sourceFS, child)) {
          LOG.debug("Traversing non-empty source dir: {}", sourceStatus.getPath());
          pathStack.push(child);
        }
      }
    }
  }

  private void writeToFileListing(
      SequenceFile.Writer fileListWriter,
      CopyListingFileStatus fileStatus,
      Path sourcePathRoot,
      S3MapReduceCpOptions options)
    throws IOException {
    LOG
        .debug("REL PATH: {}, FULL PATH: {}", PathUtil.getRelativePath(sourcePathRoot, fileStatus.getPath()),
            fileStatus.getPath());

    FileStatus status = fileStatus;

    if (!shouldCopy(fileStatus.getPath(), options)) {
      return;
    }

    fileListWriter.append(new Text(PathUtil.getRelativePath(sourcePathRoot, fileStatus.getPath())), status);
    fileListWriter.sync();

    if (!fileStatus.isDirectory()) {
      totalBytesToCopy += fileStatus.getLen();
    }
    totalPaths++;
  }

}
