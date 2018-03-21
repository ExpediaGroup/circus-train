/**
 * Copyright (C) 2016-2018 Expedia Inc.
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
package com.hotels.bdp.circustrain.comparator.hive.functions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nonnull;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;

import com.hotels.bdp.circustrain.api.CircusTrainException;
import com.hotels.bdp.circustrain.comparator.hive.wrappers.PathMetadata;

public class PathToPathMetadata implements Function<Path, PathMetadata> {

  private final Configuration conf;

  public PathToPathMetadata(Configuration conf) {
    this.conf = new Configuration(conf);
  }

  @Override
  public PathMetadata apply(@Nonnull Path location) {
    try {
      FileSystem fs = location.getFileSystem(conf);
      FileStatus fileStatus = fs.getFileStatus(location);
      FileChecksum checksum = null;
      if (fileStatus.isFile()) {
        checksum = fs.getFileChecksum(location);
      }

      long modificationTime = 0;
      List<PathMetadata> childPathDescriptors = new ArrayList<>();

      if (fileStatus.isDirectory()) {
        FileStatus[] childStatuses = fs.listStatus(location);
        for (FileStatus childStatus : childStatuses) {
          childPathDescriptors.add(apply(childStatus.getPath()));
        }
      } else {
        modificationTime = fileStatus.getModificationTime();
      }

      return new PathMetadata(location, modificationTime, checksum, childPathDescriptors);
    } catch (IOException e) {
      throw new CircusTrainException("Unable to compute digest for location " + location.toString(), e);
    }
  }

}
