/**
 * Copyright (C) 2016-2017 Expedia Inc.
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
package com.hotels.bdp.circustrain.tool.vacuum;

import java.io.IOException;
import java.util.Set;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.hotels.bdp.circustrain.housekeeping.service.impl.EventIdExtractor;

final class ConsistencyCheck {

  private ConsistencyCheck() {}

  static void checkMetastorePaths(Set<Path> paths, int globDepth) {
    for (Path path : paths) {
      checkMetastorePath(path, globDepth);
    }
  }

  static void checkMetastorePath(Path path, int globDepth) {
    checkPathContainsEventId(path, "metastore");
    if (path.depth() != globDepth) {
      throw new IllegalStateException(
          "ABORTING: Metastore path structure looks wrong; depth != file system glob depth: '" + path + "'.");
    }
  }

  static void checkFsPath(Path path) {
    checkPathContainsEventId(path, "file system");
  }

  static void checkUnvisitedPath(FileSystem fs, Path unvisitedMetastorePath) throws IOException {
    if (fs.exists(unvisitedMetastorePath)) {
      throw new IllegalStateException(
          "ABORTING: Metastore path not found in file system scan but does exist: '" + unvisitedMetastorePath + "'.");
    }
  }

  private static void checkPathContainsEventId(Path path, String ownerMessage) {
    String eventId = EventIdExtractor.extractFrom(path);
    if (eventId == null) {
      throw new IllegalStateException(
          "ABORTING: All " + ownerMessage + " paths should contain an event id, this one does not: '" + path + "'.");
    }
  }

}
