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
package com.hotels.bdp.circustrain.core.source;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hotels.bdp.circustrain.api.CircusTrainException;
import com.hotels.bdp.circustrain.api.SourceLocationManager;
import com.hotels.bdp.circustrain.api.event.SourceCatalogListener;

public class HdfsSnapshotLocationManager implements SourceLocationManager {
  private static final Logger LOG = LoggerFactory.getLogger(HdfsSnapshotLocationManager.class);

  interface FileSystemFactory {

    static final FileSystemFactory DEFAULT = new FileSystemFactory() {

      @Override
      public FileSystem get(Path path, Configuration conf) throws IOException {
        return path.getFileSystem(conf);
      }
    };

    FileSystem get(Path path, Configuration conf) throws IOException;
  }

  private final HiveConf sourceHiveConf;
  private final String eventId;
  private final Path sourceDataPath;
  private final Table sourceTable;
  private Path snapshotPath;
  private final List<Path> subPaths;
  private final Path copyBasePath;
  private final boolean snapshotsDisabled;
  private final FileSystemFactory fileSystemFactory;
  private final SourceCatalogListener sourceCatalogListener;

  HdfsSnapshotLocationManager(
      HiveConf sourceHiveConf,
      String eventId,
      Table sourceTable,
      boolean snapshotsDisabled,
      SourceCatalogListener sourceCatalogListener) throws IOException {
    this(sourceHiveConf, eventId, sourceTable, snapshotsDisabled, null, sourceCatalogListener);
  }

  HdfsSnapshotLocationManager(
      HiveConf sourceHiveConf,
      String eventId,
      Table sourceTable,
      boolean snapshotsDisabled,
      String tableBasePath,
      SourceCatalogListener sourceCatalogListener) throws IOException {
    this(sourceHiveConf, eventId, sourceTable, Collections.<Partition> emptyList(), snapshotsDisabled, tableBasePath,
        sourceCatalogListener);
  }

  HdfsSnapshotLocationManager(
      HiveConf sourceHiveConf,
      String eventId,
      Table sourceTable,
      List<Partition> sourcePartitions,
      boolean snapshotsDisabled,
      String tableBasePath,
      SourceCatalogListener sourceCatalogListener) throws IOException {
    this(sourceHiveConf, eventId, sourceTable, sourcePartitions, snapshotsDisabled, tableBasePath,
        FileSystemFactory.DEFAULT, sourceCatalogListener);
  }

  HdfsSnapshotLocationManager(
      HiveConf sourceHiveConf,
      String eventId,
      Table sourceTable,
      List<Partition> sourcePartitions,
      boolean snapshotsDisabled,
      String tableBasePath,
      FileSystemFactory fileSystemFactory,
      SourceCatalogListener sourceCatalogListener) throws IOException {
    this.sourceHiveConf = sourceHiveConf;
    this.eventId = eventId;
    this.sourceTable = sourceTable;
    this.snapshotsDisabled = snapshotsDisabled;
    this.sourceCatalogListener = sourceCatalogListener;
    this.fileSystemFactory = fileSystemFactory;
    String sourceDataLocation;
    if (StringUtils.isNotBlank(tableBasePath)) {
      sourceDataLocation = tableBasePath;
    } else {
      sourceDataLocation = sourceTable.getSd().getLocation();
    }
    sourceDataPath = new Path(sourceDataLocation);
    copyBasePath = createSnapshot();
    String copyBaseLocation = copyBasePath.toString();
    subPaths = calculateSubPaths(sourcePartitions, sourceDataLocation, copyBaseLocation);
  }

  static List<Path> calculateSubPaths(
      List<Partition> sourcePartitions,
      String sourceDataLocation,
      String copyBaseLocation) {
    List<Path> paths = new ArrayList<>(sourcePartitions.size());
    for (Partition partition : sourcePartitions) {
      String partitionLocation = partition.getSd().getLocation();
      String partitionBranch = partitionLocation.replace(sourceDataLocation, "");
      while (partitionBranch.startsWith("/")) {
        partitionBranch = partitionBranch.substring(1);
      }
      Path copyPartitionPath = new Path(copyBaseLocation, partitionBranch);
      paths.add(copyPartitionPath);
      LOG.debug("Added sub-path {}.", copyPartitionPath.toString());
    }
    return paths;
  }

  @Override
  public Path getTableLocation() {
    LOG.debug("Copying source data from: {}", copyBasePath.toString());
    sourceCatalogListener.resolvedSourceLocation(copyBasePath.toUri());
    return copyBasePath;
  }

  @Override
  public void cleanUpLocations() {
    if (snapshotPath != null) {
      try {
        LOG.debug("Deleting source data snapshot: {}, {}", sourceDataPath, eventId);
        FileSystem sourceFileSystem = fileSystemFactory.get(sourceDataPath, sourceHiveConf);
        sourceFileSystem.deleteSnapshot(sourceDataPath, eventId);
      } catch (IOException e) {
        LOG.error("Unable to delete source data snapshot: {}, {}", sourceDataPath, eventId, e);
      }
    }
  }

  @Override
  public List<Path> getPartitionLocations() {
    return Collections.unmodifiableList(subPaths);
  }

  @Override
  public Path getPartitionSubPath(Path partitionLocation) {
    String sourceDataPathString = sourceDataPath.toString();
    String partitionLocationString = partitionLocation.toString();
    if (!partitionLocationString.startsWith(sourceDataPathString)) {
      throw new CircusTrainException("Partition path '"
          + partitionLocationString
          + "' does not seem to belong to data source path '"
          + sourceDataPathString
          + "'");
    }

    String subPath = partitionLocationString.replace(sourceDataPathString, "");
    if (subPath.charAt(0) == '/') {
      subPath = subPath.substring(1);
    }
    return new Path(subPath);
  }

  private Path createSnapshot() throws IOException {
    LOG.debug("Source table {}.{} has its data located at {}", sourceTable.getDbName(), sourceTable.getTableName(),
        sourceDataPath);

    FileSystem fileSystem = fileSystemFactory.get(sourceDataPath, sourceHiveConf);
    Path snapshotMetaDataPath = new Path(sourceDataPath, HdfsConstants.DOT_SNAPSHOT_DIR);
    Path resolvedLocation = sourceDataPath;
    if (fileSystem.exists(snapshotMetaDataPath)) {
      if (snapshotsDisabled) {
        LOG.info("Path {} can be snapshot, but feature has been disabled.", sourceDataPath);
      } else {
        LOG.debug("Creating source data snapshot: {}, {}", sourceDataPath, eventId);
        // fileSystem.createSnapshot does not return a fully qualified URI.
        resolvedLocation = fileSystem.makeQualified(fileSystem.createSnapshot(sourceDataPath, eventId));
        snapshotPath = resolvedLocation;
      }
    } else {
      LOG.debug("Snapshots not enabled on source location: {}", sourceDataPath);
    }
    return resolvedLocation;
  }

}
