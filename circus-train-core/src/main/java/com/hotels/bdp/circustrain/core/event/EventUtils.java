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

package com.hotels.bdp.circustrain.core.event;

import java.net.URI;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;

import com.hotels.bdp.circustrain.api.event.EventMetastoreTunnel;
import com.hotels.bdp.circustrain.api.event.EventPartition;
import com.hotels.bdp.circustrain.api.event.EventPartitions;
import com.hotels.bdp.circustrain.api.event.EventReplicaCatalog;
import com.hotels.bdp.circustrain.api.event.EventReplicaTable;
import com.hotels.bdp.circustrain.api.event.EventS3;
import com.hotels.bdp.circustrain.api.event.EventSourceCatalog;
import com.hotels.bdp.circustrain.api.event.EventSourceTable;
import com.hotels.bdp.circustrain.api.event.EventTable;
import com.hotels.bdp.circustrain.api.event.EventTableReplication;
import com.hotels.bdp.circustrain.conf.ReplicaCatalog;
import com.hotels.bdp.circustrain.conf.Security;
import com.hotels.bdp.circustrain.conf.SourceCatalog;
import com.hotels.bdp.circustrain.conf.SourceTable;
import com.hotels.bdp.circustrain.conf.TableReplication;
import com.hotels.hcommon.hive.metastore.util.FieldSchemaUtils;
import com.hotels.hcommon.hive.metastore.util.LocationUtils;

public class EventUtils {

  public static final String EVENT_ID_UNAVAILABLE = "event-id-unavailable";

  public static List<URI> toUris(List<Path> paths) {
    if (paths == null) {
      return null;
    }
    List<URI> uris = new ArrayList<>(paths.size());
    for (Path path : paths) {
      uris.add(path.toUri());
    }
    return uris;
  }

  public static EventPartitions toEventPartitions(Table table, List<Partition> partitions) {
    LinkedHashMap<String, String> partitionKeyTypes = new LinkedHashMap<>();
    List<FieldSchema> partitionKeys = table.getPartitionKeys();
    for (FieldSchema partitionKey : partitionKeys) {
      partitionKeyTypes.put(partitionKey.getName(), partitionKey.getType());
    }
    EventPartitions eventPartitions = new EventPartitions(partitionKeyTypes);
    if (partitions != null) {
      for (Partition partition : partitions) {
        eventPartitions.add(new EventPartition(partition.getValues(),
            LocationUtils.hasLocation(partition) ? LocationUtils.locationAsUri(partition) : null));
      }
    }
    return eventPartitions;
  }

  public static EventTable toEventTable(Table sourceTable) {
    if (sourceTable == null) {
      return null;
    }
    return new EventTable(FieldSchemaUtils.getFieldNames(sourceTable.getPartitionKeys()),
        LocationUtils.hasLocation(sourceTable) ? LocationUtils.locationAsUri(sourceTable) : null);
  }

  public static EventSourceCatalog toEventSourceCatalog(SourceCatalog sourceCatalog) {
    return new EventSourceCatalog(sourceCatalog.getName(), sourceCatalog.isDisableSnapshots(),
        sourceCatalog.getSiteXml(), sourceCatalog.getConfigurationProperties());
  }

  public static EventReplicaCatalog toEventReplicaCatalog(ReplicaCatalog replicaCatalog, Security security) {
    EventS3 eventS3 = security.getCredentialProvider() == null ? null : new EventS3(security.getCredentialProvider());

    EventMetastoreTunnel tunnel = replicaCatalog.getMetastoreTunnel() == null ? null
        : new EventMetastoreTunnel(replicaCatalog.getMetastoreTunnel().getRoute(),
            replicaCatalog.getMetastoreTunnel().getPort(), replicaCatalog.getMetastoreTunnel().getLocalhost(),
            replicaCatalog.getMetastoreTunnel().getPrivateKeys(), replicaCatalog.getMetastoreTunnel().getKnownHosts());

    return new EventReplicaCatalog(replicaCatalog.getName(), replicaCatalog.getHiveMetastoreUris(), eventS3, tunnel,
        replicaCatalog.getSiteXml(), replicaCatalog.getConfigurationProperties());
  }

  public static EventTableReplication toEventTableReplication(TableReplication tableReplication) {
    SourceTable sourceTable = tableReplication.getSourceTable();
    EventReplicaTable eventReplicaTable = new EventReplicaTable(tableReplication.getReplicaTable().getDatabaseName(),
        tableReplication.getReplicaTable().getTableName(), tableReplication.getReplicaTable().getTableLocation());
    return new EventTableReplication(
        new EventSourceTable(sourceTable.getDatabaseName(), sourceTable.getTableName(), sourceTable.getTableLocation(),
            sourceTable.getPartitionFilter(), sourceTable.getPartitionLimit(), sourceTable.getQualifiedName()),
        eventReplicaTable, tableReplication.getCopierOptions(), tableReplication.getQualifiedReplicaName(),
        tableReplication.getTransformOptions());
  }

}
