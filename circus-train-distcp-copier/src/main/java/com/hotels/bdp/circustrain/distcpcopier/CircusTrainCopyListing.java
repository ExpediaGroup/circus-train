/**
 * Copyright (C) 2016-2019 Expedia, Inc.
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
package com.hotels.bdp.circustrain.distcpcopier;

import static org.apache.hadoop.io.SequenceFile.CompressionType.NONE;
import static org.apache.hadoop.io.SequenceFile.Writer.compression;
import static org.apache.hadoop.io.SequenceFile.Writer.file;
import static org.apache.hadoop.io.SequenceFile.Writer.keyClass;
import static org.apache.hadoop.io.SequenceFile.Writer.valueClass;
import static org.apache.hadoop.io.SequenceFile.createWriter;
import static org.apache.hadoop.tools.DistCpConstants.CONF_LABEL_COPY_LISTING_CLASS;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.tools.CopyListing;
import org.apache.hadoop.tools.CopyListingFileStatus;
import org.apache.hadoop.tools.DistCpOptions;
import org.apache.hadoop.tools.SimpleCopyListing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hotels.bdp.circustrain.api.CircusTrainException;

/**
 * This class is intended for building the listings of files to copy for the partitions of a Hive table. As such, it is
 * expected that the paths returned by {@link DistCpOptions#getSourcePaths()} are all partitions in the same Hive table.
 * <p>
 * What is not explicitly currently supported, therefore, are:
 * <ul>
 * <li>Duplicate paths. i.e. multiple partitions in a table pointing to the same path.
 * <li>Paths at different levels of the same root. e.g. {@code /path/a/b} and {@code /path/a}.
 * </ul>
 * <p>
 * This class was implemented as the existing {@link CopyListing} implementations do not quite do what is required.
 * Specifically, there is no way to specify what level the relative path should be to copy over to the target.
 * {@link SimpleCopyListing} always uses the path's immediate parent as the basis for the relative path. This would be
 * fine for tables with only a single folder level for the partition but not for anything deeper.
 * <p>
 * e.g. With {@link SimpleCopyListing}, specifying {@code /source/foo/bar} as the source path and {@code /target} as the
 * target, the result would be {@code /target/bar}. This is clearly not desirable as the {@code foo} folder has
 * disappeared.
 * <p>
 * With {@link CircusTrainCopyListing}, the above information is provided along with a source root path. In this case it
 * would be {@code /source}. The result would then be {@code /target/foo/bar}.
 */
public class CircusTrainCopyListing extends SimpleCopyListing {

  private static final Logger LOG = LoggerFactory.getLogger(CircusTrainCopyListing.class);

  static final String CONF_ROOT_PATH = CircusTrainCopyListing.class + "_ROOT_PATH";

  static void setAsCopyListingClass(Configuration conf) {
    conf.setClass(CONF_LABEL_COPY_LISTING_CLASS, CircusTrainCopyListing.class, CopyListing.class);
  }

  static void setRootPath(Configuration conf, Path path) {
    conf.set(CONF_ROOT_PATH, path.toUri().toString());
  }

  static Path getRootPath(Configuration conf) {
    String pathString = conf.get(CONF_ROOT_PATH);
    if (pathString == null) {
      throw new CircusTrainException("No root path was set.");
    }
    return new Path(pathString);
  }

  public CircusTrainCopyListing(Configuration configuration, Credentials credentials) {
    super(configuration, credentials);
  }

  @Override
  public void doBuildListing(Path pathToListFile, DistCpOptions options) throws IOException {
    try (Writer writer = newWriter(pathToListFile)) {

      Path sourceRootPath = getRootPath(getConf());

      for (Path sourcePath : options.getSourcePaths()) {

        FileSystem fileSystem = sourcePath.getFileSystem(getConf());
        FileStatus directory = fileSystem.getFileStatus(sourcePath);

        Map<String, CopyListingFileStatus> children = new FileStatusTreeTraverser(fileSystem)
            .preOrderTraversal(directory)
            .transform(new CopyListingFileStatusFunction(fileSystem, options))
            .uniqueIndex(new RelativePathFunction(sourceRootPath));

        for (Entry<String, CopyListingFileStatus> entry : children.entrySet()) {
          LOG.debug("Adding '{}' with relative path '{}'", entry.getValue().getPath(), entry.getKey());
          writer.append(new Text(entry.getKey()), entry.getValue());
          writer.sync();
        }
      }
    }
  }

  private Writer newWriter(Path pathToListFile) throws IOException {
    FileSystem fs = pathToListFile.getFileSystem(getConf());
    if (fs.exists(pathToListFile)) {
      fs.delete(pathToListFile, false);
    }
    return createWriter(getConf(), file(pathToListFile), keyClass(Text.class), valueClass(CopyListingFileStatus.class),
        compression(NONE));
  }

}
