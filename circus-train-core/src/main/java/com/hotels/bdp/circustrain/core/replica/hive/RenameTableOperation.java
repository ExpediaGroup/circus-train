package com.hotels.bdp.circustrain.core.replica.hive;

import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hotels.hcommon.hive.metastore.client.api.CloseableMetaStoreClient;

public class RenameTableOperation {

  private final static Logger LOG = LoggerFactory.getLogger(RenameTableOperation.class);

  /**
   * <p>
   * NOTE: assumes both `from` and `to` exist
   * </p>
   * Renames tables 'from' table into 'to' table, at the end of the operation 'from' will be gone and 'to' will be
   * renamed.
   */
  public void execute(CloseableMetaStoreClient client, Table from, Table to) throws TException {
    LOG
        .info("Renaming table {}.{} to {}.{}", from.getDbName(), from.getTableName(), to.getDbName(),
            to.getTableName());
    from = client.getTable(from.getDbName(), from.getTableName());
    to = client.getTable(to.getDbName(), to.getTableName());
    String fromTableName = from.getTableName();
    String toTableName = to.getTableName();
    String toTableNameTemp = toTableName + "_original";
    try {
      from.setTableName(toTableName);
      to.setTableName(toTableNameTemp);
      client.alter_table(to.getDbName(), toTableName, to);
      client.alter_table(from.getDbName(), fromTableName, from);
    } finally {
      client.dropTable(to.getDbName(), toTableNameTemp, false, true);
    }
  }
}
