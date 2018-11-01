package com.hotels.bdp.circustrain.core;

import org.apache.thrift.TException;

import com.hotels.bdp.circustrain.api.CircusTrainException;
import com.hotels.bdp.circustrain.api.Replication;
import com.hotels.bdp.circustrain.core.replica.DestructiveReplica;
import com.hotels.bdp.circustrain.core.source.DestructiveSource;

public class DestructiveReplication implements Replication {

  private final Replication upsertReplication;
  private final DestructiveSource destructiveSource;
  private final DestructiveReplica destructiveReplica;

  public DestructiveReplication(
      Replication upsertReplication,
      DestructiveSource destructiveSource,
      DestructiveReplica destructiveReplica) {
    this.upsertReplication = upsertReplication;
    this.destructiveSource = destructiveSource;
    this.destructiveReplica = destructiveReplica;
  }

  @Override
  public void replicate() throws CircusTrainException {
    try {
      if (destructiveReplica.tableIsUnderCircusTrainControl()) {
        throw new CircusTrainException("Replica table '"
            + destructiveReplica.getQualifiedTableName()
            + "' is not controlled by circus train aborting replication, check configuration for correct replica name");
      }
      if (destructiveSource.tableExists()) {
        destructiveReplica.dropDeletedPartitions(destructiveSource.getPartitionNames());
        // do normal replication
        upsertReplication.replicate();
      } else {
        destructiveReplica.dropTable();
      }
    } catch (TException e) {
      throw new CircusTrainException(e);
    }
  }

  @Override
  public String name() {
    return upsertReplication.name();
  }

  @Override
  public String getEventId() {
    return upsertReplication.getEventId();
  }

}
