/**
 * Copyright (C) 2016-2019 Expedia Inc.
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
package com.hotels.bdp.circustrain.core;

import org.apache.thrift.TException;

import com.hotels.bdp.circustrain.api.CircusTrainException;
import com.hotels.bdp.circustrain.api.Replication;
import com.hotels.bdp.circustrain.api.conf.TableReplication;
import com.hotels.bdp.circustrain.core.annotation.HiveTableAnnotator;
import com.hotels.bdp.circustrain.core.replica.DestructiveReplica;
import com.hotels.bdp.circustrain.core.source.DestructiveSource;

public class DestructiveReplication implements Replication {

  private final ReplicationFactoryImpl upsertReplicationFactory;
  private final TableReplication tableReplication;
  private final DestructiveSource destructiveSource;
  private final DestructiveReplica destructiveReplica;
  private final String eventId;
  private final HiveTableAnnotator hiveTableAnnotator;

  public DestructiveReplication(
      ReplicationFactoryImpl upsertReplicationFactory,
      TableReplication tableReplication,
      String eventId,
      DestructiveSource destructiveSource,
      DestructiveReplica destructiveReplica,
      HiveTableAnnotator hiveTableAnnotator) {
    this.upsertReplicationFactory = upsertReplicationFactory;
    this.tableReplication = tableReplication;
    this.eventId = eventId;
    this.destructiveSource = destructiveSource;
    this.destructiveReplica = destructiveReplica;
    this.hiveTableAnnotator = hiveTableAnnotator;
  }

  @Override
  public void replicate() throws CircusTrainException {
    try {
      if (!destructiveReplica.tableIsUnderCircusTrainControl()) {
        throw new CircusTrainException("Replica table '"
            + tableReplication.getQualifiedReplicaName()
            + "' is not controlled by circus train aborting replication, check configuration for correct replica name");
      }

      hiveTableAnnotator.annotateTable(tableReplication.getReplicaDatabaseName(),
        tableReplication.getReplicaTableName(), tableReplication.getReplicaTable().getAnnotations());

      if (destructiveSource.tableExists()) {
        destructiveReplica.dropDeletedPartitions(destructiveSource.getPartitionNames());
        // do normal replication
        Replication replication = upsertReplicationFactory.newInstance(tableReplication);
        replication.replicate();
      } else {
        destructiveReplica.dropTable();
      }
    } catch (TException e) {
      throw new CircusTrainException(e);
    }
  }

  @Override
  public String name() {
    return "destructive-" + tableReplication.getQualifiedReplicaName();
  }

  @Override
  public String getEventId() {
    return eventId;
  }

}
