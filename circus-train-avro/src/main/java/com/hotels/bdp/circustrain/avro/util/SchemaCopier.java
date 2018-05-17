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
package com.hotels.bdp.circustrain.avro.util;

import static org.apache.commons.lang.StringUtils.isBlank;

import static com.google.common.base.Preconditions.checkNotNull;

import static com.hotels.bdp.circustrain.avro.util.AvroStringUtils.fileName;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;

import com.hotels.bdp.circustrain.api.CircusTrainException;

@Component
public class SchemaCopier {

  private static final Logger LOG = LoggerFactory.getLogger(SchemaCopier.class);
  private static final String NAMESERVICES_PARAMETER = "dfs.nameservices";
  private static final String HDFS_SCHEME = "hdfs";
  private static final String S3_SCHEME = "s3";
  private static final String GS_SCHEME = "gs";
  private static final List<String> SCHEMES = ImmutableList.of(HDFS_SCHEME, S3_SCHEME, GS_SCHEME);

  private final Configuration sourceHiveConf;
  private final Configuration replicaHiveConf;

  @Autowired
  public SchemaCopier(Configuration sourceHiveConf, Configuration replicaHiveConf) {
    this.sourceHiveConf = sourceHiveConf;
    this.replicaHiveConf = replicaHiveConf;
  }

  public Path copy(String source, String destination) {
    checkNotNull(source, "source cannot be null");
    checkNotNull(destination, "destinationFolder cannot be null");

    java.nio.file.Path temporaryDirectory = createTempDirectory();

    Path localLocation = new Path(temporaryDirectory.toString(), fileName(source));
    tryCopyToLocal(source, localLocation);

    String destinationNameService = replicaHiveConf.get(NAMESERVICES_PARAMETER);
    Path destinationLocation = locationWithNameService(new Path(destination, fileName(source)).toString(),
        destinationNameService);
    copyToRemote(localLocation, destinationLocation);

    LOG.info("Avro schema has been copied from '{}' to '{}'", source, destinationLocation);
    return destinationLocation;
  }

  private void tryCopyToLocal(String source, Path localLocation) {
    String sourceNameService = sourceHiveConf.get(NAMESERVICES_PARAMETER);
    Path sourceLocation = locationWithNameService(source, sourceNameService);
    LOG.info("Attempting to copy from supported filesystems");
    try {
      copyToLocal(sourceLocation, localLocation);
    } catch (Exception e) {
      LOG.info("Couldn't copy from {} to {}, attempting copy from different filesystems", sourceLocation,
          localLocation);
      tryCopyToLocalFromSupportedFileSystems(sourceNameService, URI.create(source).getPath(), localLocation);
    }
  }

  private void tryCopyToLocalFromSupportedFileSystems(String authority, String path, Path localLocation) {
    for (String scheme : SCHEMES) {
      boolean succeeded = executeCopyToLocal(scheme, authority, path, localLocation)
          || executeCopyToLocal(scheme, authority, StringUtils.removeStart(path, "/"), localLocation);
      if (succeeded) {
        return;
      }
    }
    throw new UnsupportedOperationException("Cannot copy from avro.schema.url " + path + ", unsupported filesystem ");
  }

  private boolean executeCopyToLocal(String scheme, String authority, String path, Path destination) {
    try {
      Path source = new Path(scheme, authority, path);
      LOG.info("Attempting copy from {} to {}", source, destination);
      copyToLocal(source, destination);
      LOG.info("Copy from {} to {} succeeded", source, destination);
      return true;
    } catch (Exception e) {
      LOG.info("Could not locate avro schema in {}", scheme);
      return false;
    }
  }

  @VisibleForTesting
  Path locationWithNameService(String url, String nameService) {
    Path location;
    if (isBlank(nameService)) {
      location = new Path(url);
    } else {
      URI uri = URI.create(url);
      String scheme = uri.getScheme();
      String path = uri.getPath();
      if (isBlank(scheme)) {
        path = "/" + nameService + path;
        location = new Path(path);
      } else {
        location = new Path(scheme, nameService, path);
      }
    }
    return location;
  }

  private java.nio.file.Path createTempDirectory() {
    java.nio.file.Path temporaryDirectory = null;
    try {
      temporaryDirectory = Files.createTempDirectory("avro-schema-download-folder");
      temporaryDirectory.toFile().deleteOnExit();
    } catch (IOException e) {
      throw new CircusTrainException("Couldn't create temporaryDirectory " + temporaryDirectory, e);
    }
    return temporaryDirectory;
  }

  private void copyToLocal(Path sourceLocation, Path localLocation) {
    FileSystem sourceFileSystem;
    try {
      sourceFileSystem = sourceLocation.getFileSystem(sourceHiveConf);
      sourceFileSystem.copyToLocalFile(false, sourceLocation, localLocation);
    } catch (IOException e) {
      throw new CircusTrainException("Couldn't copy file from " + sourceLocation + " to " + localLocation, e);
    }
  }

  private void copyToRemote(Path localLocation, Path remoteDestinationLocation) {
    FileSystem destinationFileSystem;
    try {
      destinationFileSystem = remoteDestinationLocation.getFileSystem(replicaHiveConf);
      destinationFileSystem.copyFromLocalFile(localLocation, remoteDestinationLocation);
    } catch (IOException e) {
      throw new CircusTrainException("Couldn't copy file from " + localLocation + " to " + remoteDestinationLocation,
          e);
    }
  }
}
