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
package com.hotels.bdp.circustrain.distcpcopier;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.MapUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.tools.DistCpOptions;
import org.apache.hadoop.tools.DistCpOptions.FileAttribute;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hotels.bdp.circustrain.core.util.MoreMapUtils;

public class DistCpOptionsParser {

  private static final Logger LOG = LoggerFactory.getLogger(DistCpOptionsParser.class);

  public static final String FILE_ATTRIBUTES = "file-attribute"; // list of DistCpOptions.FileAttribute
  public static final String PRESERVE_RAW_XATTRS = "preserve-raw-xattrs"; // boolean
  public static final String ATOMIC_COMMIT = "atomic-commit"; // boolean
  public static final String ATOMIC_WORK_PATH = "atomic-work-path"; // Path
  public static final String COPY_STRATEGY = "copy-strategy"; // string
  public static final String IGNORE_FAILURES = "ignore-failures"; // boolean
  public static final String LOG_PATH = "log-path"; // Path
  public static final String TASK_BANDWIDTH = "task-bandwidth"; // int
  public static final String MAX_MAPS = "max-maps"; // int
  public static final String SKIP_CRC = "skip-crc"; // boolean
  public static final String SSL_CONFIGURATION_FILE = "ssl-configuration-file"; // string

  private final DistCpOptions distCpOptions;

  DistCpOptionsParser(List<Path> sourceDataLocations, Path replicaDataLocation) {
    distCpOptions = new DistCpOptions(sourceDataLocations, replicaDataLocation);
  }

  protected DistCpOptions parse(Map<String, Object> copierOptions) {
    if (copierOptions == null) {
      LOG.debug("Null copier options: nothing to parse");
      return distCpOptions;
    }

    List<FileAttribute> fileAttributes = MoreMapUtils.getListOfEnum(copierOptions, FILE_ATTRIBUTES,
        Collections.<FileAttribute>emptyList(), FileAttribute.class);
    for (FileAttribute fileAttribute : fileAttributes) {
      distCpOptions.preserve(fileAttribute);
    }
    if (MapUtils.getBoolean(copierOptions, PRESERVE_RAW_XATTRS, distCpOptions.shouldPreserveRawXattrs())) {
      distCpOptions.preserveRawXattrs();
    }
    distCpOptions.setAtomicWorkPath(
        MoreMapUtils.getHadoopPath(copierOptions, ATOMIC_WORK_PATH, distCpOptions.getAtomicWorkPath()));
    distCpOptions.setCopyStrategy(MapUtils.getString(copierOptions, COPY_STRATEGY, distCpOptions.getCopyStrategy()));
    distCpOptions
        .setIgnoreFailures(MapUtils.getBoolean(copierOptions, IGNORE_FAILURES, distCpOptions.shouldIgnoreFailures()));
    distCpOptions.setLogPath(MoreMapUtils.getHadoopPath(copierOptions, LOG_PATH, distCpOptions.getLogPath()));

    int taskBandwidth = MapUtils.getIntValue(copierOptions, TASK_BANDWIDTH, distCpOptions.getMapBandwidth());
    if (taskBandwidth <= 0) {
      throw new IllegalArgumentException("Parameter " + TASK_BANDWIDTH + " must be a positive integer.");
    }
    distCpOptions.setMapBandwidth(taskBandwidth);

    int maxMaps = MapUtils.getIntValue(copierOptions, MAX_MAPS, distCpOptions.getMaxMaps());
    if (maxMaps <= 0) {
      throw new IllegalArgumentException("Parameter " + MAX_MAPS + " must be a positive integer.");
    }
    distCpOptions.setMaxMaps(maxMaps);

    distCpOptions.setSslConfigurationFile(
        MapUtils.getString(copierOptions, SSL_CONFIGURATION_FILE, distCpOptions.getSslConfigurationFile()));
    // These validate: order is important
    distCpOptions
        .setAtomicCommit(MapUtils.getBoolean(copierOptions, ATOMIC_COMMIT, distCpOptions.shouldAtomicCommit()));
    distCpOptions.setSkipCRC(MapUtils.getBoolean(copierOptions, SKIP_CRC, distCpOptions.shouldSkipCRC()));
    return distCpOptions;
  }

}
