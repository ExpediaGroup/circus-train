package com.hotels.bdp.circustrain.core.source;

import java.util.List;

import org.apache.thrift.TException;

import com.google.common.base.Supplier;

import com.hotels.bdp.circustrain.api.conf.TableReplication;
import com.hotels.hcommon.hive.metastore.client.api.CloseableMetaStoreClient;

public class DestructiveSource {

  private static final short ALL = -1;
  private final Supplier<CloseableMetaStoreClient> sourceMetaStoreClientSupplier;
  private final String databaseName;
  private final String tableName;

  public DestructiveSource(
      Supplier<CloseableMetaStoreClient> sourceMetaStoreClientSupplier,
      TableReplication tableReplication) {
    this.sourceMetaStoreClientSupplier = sourceMetaStoreClientSupplier;
    databaseName = tableReplication.getSourceTable().getDatabaseName();
    tableName = tableReplication.getSourceTable().getTableName();
  }

  public boolean tableExists() throws TException {
    try (CloseableMetaStoreClient client = sourceMetaStoreClientSupplier.get()) {
      return client.tableExists(databaseName, tableName);
    }
  }

  public List<String> getPartitionNames() throws TException {
    try (CloseableMetaStoreClient client = sourceMetaStoreClientSupplier.get()) {
      return client.listPartitionNames(databaseName, tableName, ALL);
    }
  }

}
