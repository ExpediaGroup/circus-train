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
package com.hotels.bdp.circustrain.core.source;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hotels.bdp.circustrain.api.CircusTrainException;
import com.hotels.bdp.circustrain.api.SourceLocationManager;

public class FilterMissingPartitionsLocationManager implements SourceLocationManager {

  private final static Logger LOG = LoggerFactory.getLogger(FilterMissingPartitionsLocationManager.class);

  private final SourceLocationManager sourceLocationManager;
  private final HiveConf hiveConf;

  public FilterMissingPartitionsLocationManager(SourceLocationManager sourceLocationManager, HiveConf hiveConf) {
    this.sourceLocationManager = sourceLocationManager;
    this.hiveConf = hiveConf;
  }

  @Override
  public Path getTableLocation() throws CircusTrainException {
    return sourceLocationManager.getTableLocation();
  }

  @Override
  public List<Path> getPartitionLocations() throws CircusTrainException {
    List<Path> result = new ArrayList<>();
    List<Path> paths = sourceLocationManager.getPartitionLocations();
    FileSystem fileSystem = null;
    for (Path path : paths) {
      try {
        if (fileSystem == null) {
          fileSystem = path.getFileSystem(hiveConf);
        }
        if (fileSystem.exists(path)) {
          result.add(path);
        } else {
          LOG
              .warn("Source path '{}' does not exist skipping it for replication."
                  + " WARNING: this means there is a partition in Hive that does not have a corresponding folder in"
                  + " source file store, check your table and data.", path);
        }
      } catch (IOException e) {
        LOG.warn("Exception while checking path, skipping path '{}',  error {}", path, e);
      }
    }
    return result;
  }

  @Override
  public void cleanUpLocations() throws CircusTrainException {
    sourceLocationManager.cleanUpLocations();
  }

  @Override
  public Path getPartitionSubPath(Path partitionLocation) {
    return sourceLocationManager.getPartitionSubPath(partitionLocation);
  }

}
