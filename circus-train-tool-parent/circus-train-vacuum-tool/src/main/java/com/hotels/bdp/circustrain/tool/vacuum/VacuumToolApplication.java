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
package com.hotels.bdp.circustrain.tool.vacuum;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Supplier;

import com.hotels.hcommon.hive.metastore.client.api.CloseableMetaStoreClient;
import com.hotels.bdp.circustrain.core.conf.TableReplication;
import com.hotels.bdp.circustrain.core.conf.TableReplications;
import com.hotels.housekeeping.model.HousekeepingLegacyReplicaPath;
import com.hotels.housekeeping.model.LegacyReplicaPath;
import com.hotels.housekeeping.repository.LegacyReplicaPathRepository;
import com.hotels.housekeeping.service.HousekeepingService;
import com.hotels.housekeeping.service.impl.EventIdExtractor;

@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
class VacuumToolApplication implements ApplicationRunner {

  private static final Logger LOG = LoggerFactory.getLogger(VacuumToolApplication.class);

  private final HiveConf conf;
  private final Supplier<CloseableMetaStoreClient> clientSupplier;
  private final LegacyReplicaPathRepository legacyReplicaPathRepository;
  private final HousekeepingService housekeepingService;
  private final List<TableReplication> tableReplications;
  private final String vacuumEventId;
  private final boolean isDryRun;
  private final short batchSize;
  private final int expectedPathCount;

  private Set<Path> housekeepingPaths;
  private IMetaStoreClient metastore;

  @Autowired
  VacuumToolApplication(
      @Value("#{replicaHiveConf}") HiveConf conf,
      @Value("#{replicaMetaStoreClientSupplier}") Supplier<CloseableMetaStoreClient> clientSupplier,
      LegacyReplicaPathRepository legacyReplicaPathRepository,
      HousekeepingService housekeepingService,
      TableReplications replications,
      @Value("${dry-run:false}") boolean isDryRun,
      @Value("${partition-batch-size:1000}") short batchSize,
      @Value("${expected-path-count:10000}") int expectedPathCount) {
    this.conf = conf;
    this.clientSupplier = clientSupplier;
    this.legacyReplicaPathRepository = legacyReplicaPathRepository;
    this.housekeepingService = housekeepingService;
    this.isDryRun = isDryRun;
    this.batchSize = batchSize;
    this.expectedPathCount = expectedPathCount;
    tableReplications = replications.getTableReplications();
    vacuumEventId = "vacuum-" + DateTime.now(DateTimeZone.UTC);
  }

  @Override
  public void run(ApplicationArguments args) {
    LOG.warn("Do not run this tool at the same time as replicating to the target table!");
    if (isDryRun) {
      LOG.warn("Dry-run only!");
    }

    try {
      metastore = clientSupplier.get();
      housekeepingPaths = fetchHousekeepingPaths(legacyReplicaPathRepository);
      for (TableReplication tableReplication : tableReplications) {
        String databaseName = tableReplication.getReplicaDatabaseName();
        String tableName = tableReplication.getReplicaTableName();
        LOG.info("Vacuuming table '{}.{}'.", databaseName, tableName);
        try {
          vacuumTable(databaseName, tableName);
        } catch (ConfigurationException e) {
          // ignored
        }
      }
    } catch (URISyntaxException | TException | IOException e) {
      throw new RuntimeException(e);
    } finally {
      if (metastore != null) {
        metastore.close();
      }
    }
  }

  @VisibleForTesting
  Set<Path> fetchHousekeepingPaths(LegacyReplicaPathRepository repository) throws URISyntaxException {
    Set<Path> paths = new HashSet<>();
    Iterable<LegacyReplicaPath> legacyPaths = repository.findAll();
    for (LegacyReplicaPath legacyPath : legacyPaths) {
      paths.add(PathUtils.normalise(new Path(new URI(legacyPath.getPath()))));
    }
    return paths;
  }

  @VisibleForTesting
  void vacuumTable(String databaseName, String tableName)
    throws MetaException, TException, NoSuchObjectException, URISyntaxException, IOException {
    Table table = metastore.getTable(databaseName, tableName);

    TablePathResolver pathResolver = TablePathResolver.Factory.newTablePathResolver(metastore, table);
    Path tableBaseLocation = pathResolver.getTableBaseLocation();
    Path globPath = pathResolver.getGlobPath();
    LOG.debug("Table base location: '{}'", tableBaseLocation);
    LOG.debug("Glob path: '{}'", globPath);
    int globDepth = globPath.depth();

    Set<Path> metastorePaths = pathResolver.getMetastorePaths(batchSize, expectedPathCount);
    ConsistencyCheck.checkMetastorePaths(metastorePaths, globDepth);
    Set<Path> unvisitedMetastorePaths = new HashSet<>(metastorePaths);

    FileSystem fs = tableBaseLocation.getFileSystem(conf);
    FileStatus[] listStatus = fs.globStatus(globPath);
    Set<Path> pathsToRemove = new HashSet<>(listStatus.length);
    int metaStorePathCount = 0;
    int housekeepingPathCount = 0;
    for (FileStatus fileStatus : listStatus) {
      Path fsPath = PathUtils.normalise(fileStatus.getPath());
      ConsistencyCheck.checkFsPath(fsPath);
      if (metastorePaths.contains(fsPath)) {
        LOG.info("KEEP path '{}', referenced in the metastore.", fsPath);
        unvisitedMetastorePaths.remove(fsPath);
        metaStorePathCount++;
      } else if (housekeepingPaths.contains(fsPath)) {
        LOG.info("KEEP path '{}', referenced in housekeeping.", fsPath);
        housekeepingPathCount++;
      } else {
        pathsToRemove.add(fsPath);
      }
    }
    for (Path unvisitedMetastorePath : unvisitedMetastorePaths) {
      LOG.warn("Metastore path '{}' references non-existent data!", unvisitedMetastorePath);
      ConsistencyCheck.checkUnvisitedPath(fs, unvisitedMetastorePath);
    }
    for (Path toRemove : pathsToRemove) {
      removePath(toRemove);
    }
    LOG.info("Table '{}' vacuum path summary; filesystem: {}, metastore: {}, housekeeping: {}, remove: {}.",
        Warehouse.getQualifiedName(table), listStatus.length, metaStorePathCount, housekeepingPathCount,
        pathsToRemove.size());
  }

  @VisibleForTesting
  void removePath(Path toRemove) {
    LOG.info("REMOVE path '{}', dereferenced and can be deleted.", toRemove);
    if (!isDryRun) {
      String previousEventId = EventIdExtractor.extractFrom(toRemove);
      housekeepingService.scheduleForHousekeeping(
          new HousekeepingLegacyReplicaPath(vacuumEventId, previousEventId, toRemove.toUri().toString()));
      LOG.info("Scheduled path '{}' for deletion.", toRemove);
    } else {
      LOG.warn("DRY RUN ENABLED: path '{}' left as is.", toRemove);
    }
  }

}
