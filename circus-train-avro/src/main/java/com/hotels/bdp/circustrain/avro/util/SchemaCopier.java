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

import static com.google.common.base.Preconditions.checkNotNull;

import static com.hotels.bdp.circustrain.avro.util.AvroStringUtils.fileName;

import java.io.IOException;
import java.nio.file.Files;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import com.hotels.bdp.circustrain.api.CircusTrainException;
import com.hotels.bdp.circustrain.api.Modules;

@Profile({ Modules.REPLICATION })
@Component
public class SchemaCopier {

  private static final Logger LOG = LoggerFactory.getLogger(SchemaCopier.class);

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

    FileSystemPathResolver sourceFileSystemPathResolver = new FileSystemPathResolver(sourceHiveConf);
    Path sourceLocation = new Path(source);
    sourceLocation = sourceFileSystemPathResolver.resolveScheme(sourceLocation);
    sourceLocation = sourceFileSystemPathResolver.resolveNameServices(sourceLocation);
    Path localLocation = new Path(temporaryDirectory.toString(), fileName(source));
    copyToLocal(sourceLocation, localLocation);

    Path destinationLocation = new Path(destination, fileName(source));
    copyToRemote(localLocation, destinationLocation);

    LOG.info("Avro schema has been copied from '{}' to '{}'", sourceLocation, destinationLocation);
    return destinationLocation;
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
    try {
      FileSystem sourceFileSystem;
      sourceFileSystem = sourceLocation.getFileSystem(sourceHiveConf);
      sourceFileSystem.copyToLocalFile(false, sourceLocation, localLocation);
      LOG.info("Copy from {} to {} succeeded", sourceLocation, localLocation);
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
