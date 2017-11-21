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
package com.hotels.bdp.circustrain.housekeeping.service.impl;

import static java.lang.String.format;

import java.io.IOException;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.orm.ObjectOptimisticLockingFailureException;

import com.hotels.bdp.circustrain.api.CircusTrainException;
import com.hotels.bdp.circustrain.housekeeping.model.LegacyReplicaPath;
import com.hotels.bdp.circustrain.housekeeping.repository.LegacyReplicaPathRepository;
import com.hotels.bdp.circustrain.housekeeping.service.HousekeepingService;

public class FileSystemHousekeepingService implements HousekeepingService {
  private static final Logger LOG = LoggerFactory.getLogger(FileSystemHousekeepingService.class);

  private final LegacyReplicaPathRepository legacyReplicaPathRepository;

  private final Configuration conf;

  public FileSystemHousekeepingService(LegacyReplicaPathRepository legacyReplicaPathRepository, Configuration conf) {
    this.legacyReplicaPathRepository = legacyReplicaPathRepository;
    this.conf = conf;
    // TODO remove this when there are no more records around that hit this.
    LOG.warn("{}.fixIncompleteRecord(LegacyReplicaPath) should be removed in future.", getClass());
  }

  @Override
  public void cleanUp(Instant referenceTime) {
    try {
      List<LegacyReplicaPath> pathsToDelete = legacyReplicaPathRepository
          .findByCreationTimestampLessThanEqual(referenceTime.getMillis());
      for (LegacyReplicaPath cleanUpPath : pathsToDelete) {
        cleanUpPath = fixIncompleteRecord(cleanUpPath);
        LOG.debug("Deleting path '{}' from file system", cleanUpPath);
        Path path = new Path(cleanUpPath.getPath());
        FileSystem fs = path.getFileSystem(conf);
        Path rootPath;
        try {
          fs.delete(path, true);
          rootPath = deleteParents(fs, path, cleanUpPath.getPathEventId());
          LOG.debug("Path '{}' has been deleted from file system", cleanUpPath);
        } catch (Exception e) {
          LOG.warn("Unable to delete path '{}' from file system. Will try next time", cleanUpPath, e);
          continue;
        }
        if (oneOfMySiblingsWillTakeCareOfMyAncestors(path, rootPath, fs) || thereIsNothingMoreToDelete(fs, rootPath)) {
          // BEWARE the eventual consistency of your blobstore!
          try {
            LOG.debug("Deleting path '{}' from database", cleanUpPath);
            legacyReplicaPathRepository.delete(cleanUpPath);
          } catch (ObjectOptimisticLockingFailureException e) {
            LOG.debug(
                "Failed to delete path '{}': probably already cleaned up by process running at same time. Ok to ignore",
                cleanUpPath);
          }
        }
      }
    } catch (Exception e) {
      throw new CircusTrainException(format("Unable to execute housekeeping at instant %d", referenceTime.getMillis()),
          e);
    }
  }

  // TODO remove this when there are no more records around that hit this.
  private LegacyReplicaPath fixIncompleteRecord(LegacyReplicaPath cleanUpPath) {
    Path path = new Path(cleanUpPath.getPath());
    if (StringUtils.isBlank(cleanUpPath.getPathEventId())) {
      String previousEventId = EventIdExtractor.extractFrom(path);
      if (previousEventId != null) {
        LOG.debug("Fixing path event for path '{}' -> '{}'.", path, previousEventId);
        cleanUpPath.setPathEventId(previousEventId);
      }
    }
    return cleanUpPath;
  }

  private boolean thereIsNothingMoreToDelete(FileSystem fs, Path rootPath) throws IOException {
    return !fs.exists(rootPath);
  }

  private boolean oneOfMySiblingsWillTakeCareOfMyAncestors(Path path, Path rootPath, FileSystem fs) throws IOException {
    return !fs.exists(path) && !isEmpty(fs, rootPath);
  }

  private Path deleteParents(FileSystem fs, Path path, String eventId) throws IOException {
    if (eventId.equals(path.getName())) {
      return path;
    }
    Path parent = path.getParent();
    if (fs.exists(parent) && isEmpty(fs, parent)) {
      LOG.debug("Deleting parent path '{}'", parent);
      fs.delete(parent, false);
    }
    return deleteParents(fs, parent, eventId);
  }

  private boolean isEmpty(FileSystem fs, Path path) throws IOException {
    return !fs.exists(path) || fs.listStatus(path).length == 0;
  }

  @Override
  public void scheduleForHousekeeping(LegacyReplicaPath cleanUpPath) {
    try {
      legacyReplicaPathRepository.save(cleanUpPath);
    } catch (Exception e) {
      throw new CircusTrainException(format("Unable to schedule path %s for deletion", cleanUpPath.getPath()), e);
    }
  }

}
