/**
 * Copyright (C) 2016-2020 Expedia, Inc.
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

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import com.hotels.bdp.circustrain.api.Modules;
import com.hotels.bdp.circustrain.api.conf.TableReplication;
import com.hotels.bdp.circustrain.api.copier.Copier;
import com.hotels.bdp.circustrain.api.copier.CopierFactory;
import com.hotels.bdp.circustrain.api.copier.CopierFactoryManager;
import com.hotels.bdp.circustrain.api.copier.CopierOptions;
import com.hotels.bdp.circustrain.api.event.EventTableReplication;
import com.hotels.bdp.circustrain.api.metrics.Metrics;

@Profile({ Modules.REPLICATION })
@Component
public class SchemaCopier {

  private static final Logger LOG = LoggerFactory.getLogger(SchemaCopier.class);

  private final Configuration sourceHiveConf;
  private final CopierFactoryManager copierFactoryManager;
  private final CopierOptions globalCopierOptions;

  @Autowired
  public SchemaCopier(
      Configuration sourceHiveConf,
      CopierFactoryManager copierFactoryManager,
      CopierOptions globalCopierOptions) {
    this.sourceHiveConf = sourceHiveConf;
    this.copierFactoryManager = copierFactoryManager;
    this.globalCopierOptions = globalCopierOptions;
  }

  public Path copy(String source, String destination, EventTableReplication eventTableReplication, String eventId) {
    checkNotNull(source, "source cannot be null");
    checkNotNull(destination, "destinationFolder cannot be null");

    FileSystemPathResolver sourceFileSystemPathResolver = new FileSystemPathResolver(sourceHiveConf);
    Path sourceLocation = new Path(source);
    sourceLocation = sourceFileSystemPathResolver.resolveScheme(sourceLocation);
    sourceLocation = sourceFileSystemPathResolver.resolveNameServices(sourceLocation);

    Path destinationSchemaFile = new Path(destination, sourceLocation.getName());

    Map<String, Object> mergeCopierOptions = new HashMap<>(TableReplication
        .getMergedCopierOptions(globalCopierOptions.getCopierOptions(), eventTableReplication.getCopierOptions()));
    mergeCopierOptions.put(CopierOptions.COPY_DESTINATION_IS_FILE, "true");
    CopierFactory copierFactory = copierFactoryManager
        .getCopierFactory(sourceLocation, destinationSchemaFile, mergeCopierOptions);
    LOG.info("Going to replicate the Avro schema from '{}' to '{}'", sourceLocation, destinationSchemaFile);
    Copier copier = copierFactory.newInstance(eventId, sourceLocation, destinationSchemaFile, mergeCopierOptions);
    Metrics metrics = copier.copy();

    LOG
        .info("Avro schema '{} byes' has been copied from '{}' to '{}'", metrics.getBytesReplicated(), sourceLocation,
            destinationSchemaFile);
    return destinationSchemaFile;
  }
}
