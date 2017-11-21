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
package com.hotels.bdp.circustrain.core.metastore;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.common.ObjectPair;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.PartitionDropOptions;
import org.apache.hadoop.hive.metastore.api.AggrStats;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.CompactionType;
import org.apache.hadoop.hive.metastore.api.CurrentNotificationEventId;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.FireEventRequest;
import org.apache.hadoop.hive.metastore.api.FireEventResponse;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.GetOpenTxnsInfoResponse;
import org.apache.hadoop.hive.metastore.api.GetPrincipalsInRoleRequest;
import org.apache.hadoop.hive.metastore.api.GetPrincipalsInRoleResponse;
import org.apache.hadoop.hive.metastore.api.GetRoleGrantsForPrincipalRequest;
import org.apache.hadoop.hive.metastore.api.GetRoleGrantsForPrincipalResponse;
import org.apache.hadoop.hive.metastore.api.HeartbeatTxnRangeResponse;
import org.apache.hadoop.hive.metastore.api.HiveObjectPrivilege;
import org.apache.hadoop.hive.metastore.api.HiveObjectRef;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.NotificationEventResponse;
import org.apache.hadoop.hive.metastore.api.OpenTxnsResponse;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PartitionEventType;
import org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.PrivilegeBag;
import org.apache.hadoop.hive.metastore.api.Role;
import org.apache.hadoop.hive.metastore.api.SetPartitionsStatsRequest;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponse;
import org.apache.hadoop.hive.metastore.api.ShowLocksResponse;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.partition.spec.PartitionSpecProxy;
import org.apache.thrift.TException;

import com.jcraft.jsch.JSchException;
import com.pastdev.jsch.tunnel.TunnelConnectionManager;

import com.hotels.bdp.circustrain.api.metastore.CloseableMetaStoreClient;

class TunnellingMetaStoreClient implements CloseableMetaStoreClient {

  private final IMetaStoreClient delegate;
  private final TunnelConnectionManager tunnelConnectionManager;

  TunnellingMetaStoreClient(IMetaStoreClient delegate, TunnelConnectionManager tunnelConnectionManager) {
    this.delegate = delegate;
    this.tunnelConnectionManager = tunnelConnectionManager;
  }

  @Override
  public void close() {
    try {
      delegate.close();
    } finally {
      tunnelConnectionManager.close();
    }
  }

  // SIMPLE DELEGATION BELOW

  @Override
  public boolean isCompatibleWith(HiveConf conf) {
    return delegate.isCompatibleWith(conf);
  }

  @Override
  public void reconnect() throws MetaException {
    try {
      tunnelConnectionManager.ensureOpen();
    } catch (JSchException e) {
      throw new RuntimeException("Unable to reopen tunnel connections", e);
    }
    delegate.reconnect();
  }

  @Override
  public void setMetaConf(String key, String value) throws TException {
    delegate.setMetaConf(key, value);
  }

  @Override
  public String getMetaConf(String key) throws TException {
    return delegate.getMetaConf(key);
  }

  @Override
  public List<String> getDatabases(String databasePattern) throws TException {
    return delegate.getDatabases(databasePattern);
  }

  @Override
  public List<String> getAllDatabases() throws TException {
    return delegate.getAllDatabases();
  }

  @Override
  public List<String> getTables(String dbName, String tablePattern) throws TException {
    return delegate.getTables(dbName, tablePattern);
  }

  @Override
  public List<String> getAllTables(String dbName) throws TException {
    return delegate.getAllTables(dbName);
  }

  @Override
  public List<String> listTableNamesByFilter(String dbName, String filter, short maxTables) throws TException {
    return delegate.listTableNamesByFilter(dbName, filter, maxTables);
  }

  @Override
  public void dropTable(String dbname, String tableName, boolean deleteData, boolean ignoreUnknownTab)
    throws TException {
    delegate.dropTable(dbname, tableName, deleteData, ignoreUnknownTab);
  }

  @Override
  public void dropTable(String dbname, String tableName, boolean deleteData, boolean ignoreUnknownTab, boolean ifPurge)
    throws TException {
    delegate.dropTable(dbname, tableName, deleteData, ignoreUnknownTab, ifPurge);
  }

  @Deprecated
  @Override
  public void dropTable(String tableName, boolean deleteData) throws TException {
    delegate.dropTable(tableName, deleteData);
  }

  @Override
  public void dropTable(String dbname, String tableName) throws TException {
    delegate.dropTable(dbname, tableName);
  }

  @Override
  public boolean tableExists(String databaseName, String tableName) throws TException {
    return delegate.tableExists(databaseName, tableName);
  }

  @Deprecated
  @Override
  public boolean tableExists(String tableName) throws TException {
    return delegate.tableExists(tableName);
  }

  @Deprecated
  @Override
  public Table getTable(String tableName) throws TException {
    return delegate.getTable(tableName);
  }

  @Override
  public Database getDatabase(String databaseName) throws TException {
    return delegate.getDatabase(databaseName);
  }

  @Override
  public Table getTable(String dbName, String tableName) throws TException {
    return delegate.getTable(dbName, tableName);
  }

  @Override
  public List<Table> getTableObjectsByName(String dbName, List<String> tableNames) throws TException {
    return delegate.getTableObjectsByName(dbName, tableNames);
  }

  @Override
  public Partition appendPartition(String tableName, String dbName, List<String> partVals) throws TException {
    return delegate.appendPartition(tableName, dbName, partVals);
  }

  @Override
  public Partition appendPartition(String tableName, String dbName, String name) throws TException {
    return delegate.appendPartition(tableName, dbName, name);
  }

  @Override
  public Partition add_partition(Partition partition) throws TException {
    return delegate.add_partition(partition);
  }

  @Override
  public int add_partitions(List<Partition> partitions) throws TException {
    return delegate.add_partitions(partitions);
  }

  @Override
  public int add_partitions_pspec(PartitionSpecProxy partitionSpec) throws TException {
    return delegate.add_partitions_pspec(partitionSpec);
  }

  @Override
  public List<Partition> add_partitions(List<Partition> partitions, boolean ifNotExists, boolean needResults)
    throws TException {
    return delegate.add_partitions(partitions, ifNotExists, needResults);
  }

  @Override
  public Partition getPartition(String tableName, String dbName, List<String> partVals) throws TException {
    return delegate.getPartition(tableName, dbName, partVals);
  }

  @Override
  public Partition exchange_partition(
      Map<String, String> partitionSpecs,
      String sourceDb,
      String sourceTable,
      String destdb,
      String destTableName)
    throws TException {
    return delegate.exchange_partition(partitionSpecs, sourceDb, sourceTable, destdb, destTableName);
  }

  @Override
  public Partition getPartition(String dbName, String tableName, String name) throws TException {
    return delegate.getPartition(dbName, tableName, name);
  }

  @Override
  public Partition getPartitionWithAuthInfo(
      String dbName,
      String tableName,
      List<String> pvals,
      String userName,
      List<String> groupNames)
    throws TException {
    return delegate.getPartitionWithAuthInfo(dbName, tableName, pvals, userName, groupNames);
  }

  @Override
  public List<Partition> listPartitions(String dbName, String tableName, short maxParts) throws TException {
    return delegate.listPartitions(dbName, tableName, maxParts);
  }

  @Override
  public PartitionSpecProxy listPartitionSpecs(String dbName, String tableName, int maxParts) throws TException {
    return delegate.listPartitionSpecs(dbName, tableName, maxParts);
  }

  @Override
  public List<Partition> listPartitions(String dbName, String tableName, List<String> partitionValues, short maxParts)
    throws TException {
    return delegate.listPartitions(dbName, tableName, partitionValues, maxParts);
  }

  @Override
  public List<String> listPartitionNames(String dbName, String tableName, short maxParts) throws TException {
    return delegate.listPartitionNames(dbName, tableName, maxParts);
  }

  @Override
  public List<String> listPartitionNames(String dbName, String tableName, List<String> partitionValues, short maxParts)
    throws TException {
    return delegate.listPartitionNames(dbName, tableName, partitionValues, maxParts);
  }

  @Override
  public List<Partition> listPartitionsByFilter(String dbName, String tableName, String filter, short maxParts)
    throws TException {
    return delegate.listPartitionsByFilter(dbName, tableName, filter, maxParts);
  }

  @Override
  public PartitionSpecProxy listPartitionSpecsByFilter(String dbName, String tableName, String filter, int maxParts)
    throws TException {
    return delegate.listPartitionSpecsByFilter(dbName, tableName, filter, maxParts);
  }

  @Override
  public boolean listPartitionsByExpr(
      String dbName,
      String tableName,
      byte[] expr,
      String defaultPartitionName,
      short maxParts,
      List<Partition> result)
    throws TException {
    return delegate.listPartitionsByExpr(dbName, tableName, expr, defaultPartitionName, maxParts, result);
  }

  @Override
  public List<Partition> listPartitionsWithAuthInfo(
      String dbName,
      String tableName,
      short s,
      String userName,
      List<String> groupNames)
    throws TException {
    return delegate.listPartitionsWithAuthInfo(dbName, tableName, s, userName, groupNames);
  }

  @Override
  public List<Partition> getPartitionsByNames(String dbName, String tableName, List<String> partitionNames)
    throws TException {
    return delegate.getPartitionsByNames(dbName, tableName, partitionNames);
  }

  @Override
  public List<Partition> listPartitionsWithAuthInfo(
      String dbName,
      String tableName,
      List<String> partialPvals,
      short s,
      String userName,
      List<String> groupNames)
    throws TException {
    return delegate.listPartitionsWithAuthInfo(dbName, tableName, partialPvals, s, userName, groupNames);
  }

  @Override
  public void markPartitionForEvent(
      String dbName,
      String tableName,
      Map<String, String> partKVs,
      PartitionEventType eventType)
    throws TException {
    delegate.markPartitionForEvent(dbName, tableName, partKVs, eventType);
  }

  @Override
  public boolean isPartitionMarkedForEvent(
      String dbName,
      String tableName,
      Map<String, String> partKVs,
      PartitionEventType eventType)
    throws TException {
    return delegate.isPartitionMarkedForEvent(dbName, tableName, partKVs, eventType);
  }

  @Override
  public void validatePartitionNameCharacters(List<String> partVals) throws TException {
    delegate.validatePartitionNameCharacters(partVals);
  }

  @Override
  public void createTable(Table tbl) throws TException {
    delegate.createTable(tbl);
  }

  @Override
  public void alter_table(String defaultDatabaseName, String tableName, Table table) throws TException {
    delegate.alter_table(defaultDatabaseName, tableName, table);
  }

  @Override
  public void createDatabase(Database db) throws TException {
    delegate.createDatabase(db);
  }

  @Override
  public void dropDatabase(String name) throws TException {
    delegate.dropDatabase(name);
  }

  @Override
  public void dropDatabase(String name, boolean deleteData, boolean ignoreUnknownDb) throws TException {
    delegate.dropDatabase(name, deleteData, ignoreUnknownDb);
  }

  @Override
  public void dropDatabase(String name, boolean deleteData, boolean ignoreUnknownDb, boolean cascade)
    throws TException {
    delegate.dropDatabase(name, deleteData, ignoreUnknownDb, cascade);
  }

  @Override
  public void alterDatabase(String name, Database db) throws TException {
    delegate.alterDatabase(name, db);
  }

  @Override
  public boolean dropPartition(String dbName, String tableName, List<String> partitionValues, boolean deleteData)
    throws TException {
    return delegate.dropPartition(dbName, tableName, partitionValues, deleteData);
  }

  @Override
  public List<Partition> dropPartitions(
      String dbName,
      String tableName,
      List<ObjectPair<Integer, byte[]>> partExprs,
      boolean deleteData,
      boolean ignoreProtection,
      boolean ifExists)
    throws TException {
    return delegate.dropPartitions(dbName, tableName, partExprs, deleteData, ignoreProtection, ifExists);
  }

  @Override
  public boolean dropPartition(String dbName, String tableName, String name, boolean deleteData) throws TException {
    return delegate.dropPartition(dbName, tableName, name, deleteData);
  }

  @Override
  public void alter_partition(String dbName, String tableName, Partition newPart) throws TException {
    delegate.alter_partition(dbName, tableName, newPart);
  }

  @Override
  public void alter_partitions(String dbName, String tableName, List<Partition> newParts) throws TException {
    delegate.alter_partitions(dbName, tableName, newParts);
  }

  @Override
  public void renamePartition(String dbname, String name, List<String> partitionValues, Partition newPart)
    throws TException {
    delegate.renamePartition(dbname, name, partitionValues, newPart);
  }

  @Override
  public List<FieldSchema> getFields(String db, String tableName) throws TException {
    return delegate.getFields(db, tableName);
  }

  @Override
  public List<FieldSchema> getSchema(String db, String tableName) throws TException {
    return delegate.getSchema(db, tableName);
  }

  @Override
  public String getConfigValue(String name, String defaultValue) throws TException {
    return delegate.getConfigValue(name, defaultValue);
  }

  @Override
  public List<String> partitionNameToVals(String name) throws TException {
    return delegate.partitionNameToVals(name);
  }

  @Override
  public Map<String, String> partitionNameToSpec(String name) throws TException {
    return delegate.partitionNameToSpec(name);
  }

  @Override
  public void createIndex(Index index, Table indexTable) throws TException {
    delegate.createIndex(index, indexTable);
  }

  @Override
  public void alter_index(String dbName, String tableName, String indexName, Index index) throws TException {
    delegate.alter_index(dbName, tableName, indexName, index);
  }

  @Override
  public Index getIndex(String dbName, String tableName, String indexName) throws TException {
    return delegate.getIndex(dbName, tableName, indexName);
  }

  @Override
  public List<Index> listIndexes(String dbName, String tableName, short max) throws TException {
    return delegate.listIndexes(dbName, tableName, max);
  }

  @Override
  public List<String> listIndexNames(String dbName, String tableName, short max) throws TException {
    return delegate.listIndexNames(dbName, tableName, max);
  }

  @Override
  public boolean dropIndex(String dbName, String tableName, String name, boolean deleteData) throws TException {
    return delegate.dropIndex(dbName, tableName, name, deleteData);
  }

  @Override
  public boolean updateTableColumnStatistics(ColumnStatistics statsObj) throws TException {
    return delegate.updateTableColumnStatistics(statsObj);
  }

  @Override
  public boolean updatePartitionColumnStatistics(ColumnStatistics statsObj) throws TException {
    return delegate.updatePartitionColumnStatistics(statsObj);
  }

  @Override
  public List<ColumnStatisticsObj> getTableColumnStatistics(String dbName, String tableName, List<String> colNames)
    throws TException {
    return delegate.getTableColumnStatistics(dbName, tableName, colNames);
  }

  @Override
  public Map<String, List<ColumnStatisticsObj>> getPartitionColumnStatistics(
      String dbName,
      String tableName,
      List<String> partNames,
      List<String> colNames)
    throws TException {
    return delegate.getPartitionColumnStatistics(dbName, tableName, partNames, colNames);
  }

  @Override
  public boolean deletePartitionColumnStatistics(String dbName, String tableName, String partName, String colName)
    throws TException {
    return delegate.deletePartitionColumnStatistics(dbName, tableName, partName, colName);
  }

  @Override
  public boolean deleteTableColumnStatistics(String dbName, String tableName, String colName) throws TException {
    return delegate.deleteTableColumnStatistics(dbName, tableName, colName);
  }

  @Override
  public boolean create_role(Role role) throws TException {
    return delegate.create_role(role);
  }

  @Override
  public boolean drop_role(String roleName) throws TException {
    return delegate.drop_role(roleName);
  }

  @Override
  public List<String> listRoleNames() throws TException {
    return delegate.listRoleNames();
  }

  @Override
  public boolean grant_role(
      String roleName,
      String userName,
      PrincipalType principalType,
      String grantor,
      PrincipalType grantorType,
      boolean grantOption)
    throws TException {
    return delegate.grant_role(roleName, userName, principalType, grantor, grantorType, grantOption);
  }

  @Override
  public boolean revoke_role(String roleName, String userName, PrincipalType principalType, boolean grantOption)
    throws TException {
    return delegate.revoke_role(roleName, userName, principalType, grantOption);
  }

  @Override
  public List<Role> list_roles(String principalName, PrincipalType principalType) throws TException {
    return delegate.list_roles(principalName, principalType);
  }

  @Override
  public PrincipalPrivilegeSet get_privilege_set(HiveObjectRef hiveObject, String userName, List<String> groupNames)
    throws TException {
    return delegate.get_privilege_set(hiveObject, userName, groupNames);
  }

  @Override
  public List<HiveObjectPrivilege> list_privileges(
      String principalName,
      PrincipalType principalType,
      HiveObjectRef hiveObject)
    throws TException {
    return delegate.list_privileges(principalName, principalType, hiveObject);
  }

  @Override
  public boolean grant_privileges(PrivilegeBag privileges) throws TException {
    return delegate.grant_privileges(privileges);
  }

  @Override
  public boolean revoke_privileges(PrivilegeBag privileges, boolean grantOption) throws TException {
    return delegate.revoke_privileges(privileges, grantOption);
  }

  @Override
  public String getDelegationToken(String owner, String renewerKerberosPrincipalName) throws TException {
    return delegate.getDelegationToken(owner, renewerKerberosPrincipalName);
  }

  @Override
  public long renewDelegationToken(String tokenStrForm) throws TException {
    return delegate.renewDelegationToken(tokenStrForm);
  }

  @Override
  public void cancelDelegationToken(String tokenStrForm) throws TException {
    delegate.cancelDelegationToken(tokenStrForm);
  }

  @Override
  public void createFunction(Function func) throws TException {
    delegate.createFunction(func);
  }

  @Override
  public void alterFunction(String dbName, String funcName, Function newFunction) throws TException {
    delegate.alterFunction(dbName, funcName, newFunction);
  }

  @Override
  public void dropFunction(String dbName, String funcName) throws TException {
    delegate.dropFunction(dbName, funcName);
  }

  @Override
  public Function getFunction(String dbName, String funcName) throws TException {
    return delegate.getFunction(dbName, funcName);
  }

  @Override
  public List<String> getFunctions(String dbName, String pattern) throws TException {
    return delegate.getFunctions(dbName, pattern);
  }

  @Override
  public ValidTxnList getValidTxns() throws TException {
    return delegate.getValidTxns();
  }

  @Override
  public ValidTxnList getValidTxns(long currentTxn) throws TException {
    return delegate.getValidTxns(currentTxn);
  }

  @Override
  public long openTxn(String user) throws TException {
    return delegate.openTxn(user);
  }

  @Override
  public OpenTxnsResponse openTxns(String user, int numTxns) throws TException {
    return delegate.openTxns(user, numTxns);
  }

  @Override
  public void rollbackTxn(long txnid) throws TException {
    delegate.rollbackTxn(txnid);
  }

  @Override
  public void commitTxn(long txnid) throws TException {
    delegate.commitTxn(txnid);
  }

  @Override
  public GetOpenTxnsInfoResponse showTxns() throws TException {
    return delegate.showTxns();
  }

  @Override
  public LockResponse lock(LockRequest request) throws TException {
    return delegate.lock(request);
  }

  @Override
  public LockResponse checkLock(long lockid) throws TException {
    return delegate.checkLock(lockid);
  }

  @Override
  public void unlock(long lockid) throws TException {
    delegate.unlock(lockid);
  }

  @Override
  public ShowLocksResponse showLocks() throws TException {
    return delegate.showLocks();
  }

  @Override
  public void heartbeat(long txnid, long lockid) throws TException {
    delegate.heartbeat(txnid, lockid);
  }

  @Override
  public HeartbeatTxnRangeResponse heartbeatTxnRange(long min, long max) throws TException {
    return delegate.heartbeatTxnRange(min, max);
  }

  @Override
  public void compact(String dbname, String tableName, String partitionName, CompactionType type) throws TException {
    delegate.compact(dbname, tableName, partitionName, type);
  }

  @Override
  public ShowCompactResponse showCompactions() throws TException {
    return delegate.showCompactions();
  }

  @Override
  public GetPrincipalsInRoleResponse get_principals_in_role(GetPrincipalsInRoleRequest getPrincRoleReq)
    throws TException {
    return delegate.get_principals_in_role(getPrincRoleReq);
  }

  @Override
  public GetRoleGrantsForPrincipalResponse get_role_grants_for_principal(
      GetRoleGrantsForPrincipalRequest getRolePrincReq)
    throws TException {
    return delegate.get_role_grants_for_principal(getRolePrincReq);
  }

  @Override
  public AggrStats getAggrColStatsFor(String dbName, String tableName, List<String> colNames, List<String> partName)
    throws TException {
    return delegate.getAggrColStatsFor(dbName, tableName, colNames, partName);
  }

  @Override
  public boolean setPartitionColumnStatistics(SetPartitionsStatsRequest request) throws TException {
    return delegate.setPartitionColumnStatistics(request);
  }

  @Override
  public void setHiveAddedJars(String addedJars) {
    delegate.setHiveAddedJars(addedJars);
  }

  @Override
  public void alter_table(String defaultDatabaseName, String tblName, Table table, boolean cascade)
    throws InvalidOperationException, MetaException, TException {
    delegate.alter_table(defaultDatabaseName, tblName, table, cascade);
  }

  @Override
  public boolean dropPartition(String db_name, String tbl_name, List<String> part_vals, PartitionDropOptions options)
    throws TException {
    return delegate.dropPartition(db_name, tbl_name, part_vals, options);
  }

  @Override
  public List<Partition> dropPartitions(
      String dbName,
      String tblName,
      List<ObjectPair<Integer, byte[]>> partExprs,
      boolean deleteData,
      boolean ignoreProtection,
      boolean ifExists,
      boolean needResults)
    throws NoSuchObjectException, MetaException, TException {
    return delegate.dropPartitions(dbName, tblName, partExprs, deleteData, ignoreProtection, ifExists, needResults);
  }

  @Override
  public List<Partition> dropPartitions(
      String dbName,
      String tblName,
      List<ObjectPair<Integer, byte[]>> partExprs,
      PartitionDropOptions options)
    throws TException {
    return delegate.dropPartitions(dbName, tblName, partExprs, options);
  }

  @Override
  public String getTokenStrForm() throws IOException {
    return delegate.getTokenStrForm();
  }

  @Override
  public void addDynamicPartitions(long txnId, String dbName, String tableName, List<String> partNames)
    throws TException {
    delegate.addDynamicPartitions(txnId, dbName, tableName, partNames);
  }

  @Override
  public NotificationEventResponse getNextNotification(long lastEventId, int maxEvents, NotificationFilter filter)
    throws TException {
    return delegate.getNextNotification(lastEventId, maxEvents, filter);
  }

  @Override
  public CurrentNotificationEventId getCurrentNotificationEventId() throws TException {
    return delegate.getCurrentNotificationEventId();
  }

  @Override
  public FireEventResponse fireListenerEvent(FireEventRequest request) throws TException {
    return delegate.fireListenerEvent(request);
  }


}
