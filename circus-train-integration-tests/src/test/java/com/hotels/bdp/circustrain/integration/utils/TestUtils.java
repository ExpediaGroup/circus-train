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
package com.hotels.bdp.circustrain.integration.utils;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData._Fields;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsDesc;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.LongColumnStatsData;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.thrift.TException;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;

import com.hotels.housekeeping.model.LegacyReplicaPath;

public final class TestUtils {

  private static final Joiner COMMA_JOINER = Joiner.on(", ");

  private TestUtils() {}

  public static final String HOUSEKEEPING_DB_USER = "bdp";
  public static final String HOUSEKEEPING_DB_PASSWD = "Ch4ll3ng3";

  public static final List<FieldSchema> DATA_COLUMNS = Arrays.asList(new FieldSchema("id", "bigint", ""),
      new FieldSchema("name", "string", ""), new FieldSchema("city", "tinyint", ""));

  public static final List<FieldSchema> PARTITION_COLUMNS = Arrays.asList(new FieldSchema("continent", "string", ""),
      new FieldSchema("country", "string", ""));

  public static List<LegacyReplicaPath> getCleanUpPaths(Connection connection, String query) throws Exception {
    List<LegacyReplicaPath> result = new ArrayList<>();
    try (PreparedStatement preparedStatement = connection.prepareStatement(query);
        ResultSet resultSet = preparedStatement.executeQuery()) {
      while (resultSet.next()) {
        result.add(new LegacyReplicaPath(resultSet.getString("event_id"), resultSet.getString("path_event_id"),
            resultSet.getString("path")));
      }
    }
    return result;
  }

  public static Table createUnpartitionedTable(
      HiveMetaStoreClient metaStoreClient,
      String database,
      String table,
      URI location)
    throws TException {
    Table hiveTable = new Table();
    hiveTable.setDbName(database);
    hiveTable.setTableName(table);
    hiveTable.setTableType(TableType.EXTERNAL_TABLE.name());
    hiveTable.putToParameters("EXTERNAL", "TRUE");

    StorageDescriptor sd = new StorageDescriptor();
    sd.setCols(DATA_COLUMNS);
    sd.setLocation(location.toString());
    sd.setParameters(new HashMap<String, String>());
    sd.setInputFormat(TextInputFormat.class.getName());
    sd.setOutputFormat(TextOutputFormat.class.getName());
    sd.setSerdeInfo(new SerDeInfo());
    sd.getSerdeInfo().setSerializationLib("org.apache.hadoop.hive.serde2.OpenCSVSerde");

    hiveTable.setSd(sd);

    metaStoreClient.createTable(hiveTable);

    ColumnStatisticsDesc statsDesc = new ColumnStatisticsDesc(true, database, table);
    ColumnStatisticsData statsData = new ColumnStatisticsData(_Fields.LONG_STATS, new LongColumnStatsData(1L, 2L));
    ColumnStatisticsObj cso1 = new ColumnStatisticsObj("id", "bigint", statsData);
    List<ColumnStatisticsObj> statsObj = Collections.singletonList(cso1);
    metaStoreClient.updateTableColumnStatistics(new ColumnStatistics(statsDesc, statsObj));

    return hiveTable;
  }

  public static Table createPartitionedTable(
      HiveMetaStoreClient metaStoreClient,
      String database,
      String table,
      URI location)
    throws Exception {

    Table hiveTable = new Table();
    hiveTable.setDbName(database);
    hiveTable.setTableName(table);
    hiveTable.setTableType(TableType.EXTERNAL_TABLE.name());
    hiveTable.putToParameters("EXTERNAL", "TRUE");

    hiveTable.setPartitionKeys(PARTITION_COLUMNS);

    StorageDescriptor sd = new StorageDescriptor();
    sd.setCols(DATA_COLUMNS);
    sd.setLocation(location.toString());
    sd.setParameters(new HashMap<String, String>());
    sd.setInputFormat(TextInputFormat.class.getName());
    sd.setOutputFormat(TextOutputFormat.class.getName());
    sd.setSerdeInfo(new SerDeInfo());
    sd.getSerdeInfo().setSerializationLib("org.apache.hadoop.hive.serde2.OpenCSVSerde");

    hiveTable.setSd(sd);

    metaStoreClient.createTable(hiveTable);

    ColumnStatisticsDesc statsDesc = new ColumnStatisticsDesc(true, database, table);
    ColumnStatisticsData statsData = new ColumnStatisticsData(_Fields.LONG_STATS, new LongColumnStatsData(1L, 2L));
    ColumnStatisticsObj cso1 = new ColumnStatisticsObj("id", "bigint", statsData);
    List<ColumnStatisticsObj> statsObj = Collections.singletonList(cso1);
    metaStoreClient.updateTableColumnStatistics(new ColumnStatistics(statsDesc, statsObj));

    return hiveTable;
  }

  public static Partition newTablePartition(Table hiveTable, List<String> values, URI location) {
    Partition partition = new Partition();
    partition.setDbName(hiveTable.getDbName());
    partition.setTableName(hiveTable.getTableName());
    partition.setValues(values);
    partition.setSd(new StorageDescriptor(hiveTable.getSd()));
    partition.getSd().setLocation(location.toString());
    return partition;
  }

  private static Table createView(
      HiveMetaStoreClient metaStoreClient,
      String database,
      String view,
      String table,
      List<FieldSchema> partitionCols)
    throws TException {
    Table hiveView = new Table();
    hiveView.setDbName(database);
    hiveView.setTableName(view);
    hiveView.setTableType(TableType.VIRTUAL_VIEW.name());
    hiveView.setViewOriginalText(hql(database, table));
    hiveView.setViewExpandedText(expandHql(database, table, DATA_COLUMNS, partitionCols));
    hiveView.setPartitionKeys(partitionCols);

    StorageDescriptor sd = new StorageDescriptor();
    sd.setCols(DATA_COLUMNS);
    sd.setParameters(new HashMap<String, String>());
    sd.setSerdeInfo(new SerDeInfo());
    hiveView.setSd(sd);

    metaStoreClient.createTable(hiveView);

    return hiveView;
  }

  private static String hql(String database, String table) {
    return String.format("SELECT * FROM %s.%s", database, table);
  }

  private static String expandHql(
      String database,
      String table,
      List<FieldSchema> dataColumns,
      List<FieldSchema> partitionColumns) {
    List<String> dataColumnNames = toQualifiedColumnNames(table, dataColumns);
    List<String> partitionColumnNames = partitionColumns != null ? toQualifiedColumnNames(table, partitionColumns)
        : ImmutableList.<String> of();
    List<String> colNames = ImmutableList
        .<String> builder()
        .addAll(dataColumnNames)
        .addAll(partitionColumnNames)
        .build();

    String cols = COMMA_JOINER.join(colNames);
    return String.format("SELECT %s FROM `%s`.`%s`", cols, database, table);
  }

  private static List<String> toQualifiedColumnNames(final String table, List<FieldSchema> columns) {
    return FluentIterable.from(columns).transform(new Function<FieldSchema, String>() {
      @Override
      public String apply(FieldSchema input) {
        return String.format("`%s`.`%s`", table, input.getName());
      }
    }).toList();
  }

  public static Table createUnpartitionedView(
      HiveMetaStoreClient metaStoreClient,
      String database,
      String view,
      String table)
    throws TException {
    return createView(metaStoreClient, database, view, table, null);
  }

  public static Table createPartitionedView(
      HiveMetaStoreClient metaStoreClient,
      String database,
      String view,
      String table)
    throws Exception {
    return createView(metaStoreClient, database, view, table, PARTITION_COLUMNS);
  }

  public static Partition newViewPartition(Table hiveView, List<String> values) {
    Partition partition = new Partition();
    partition.setDbName(hiveView.getDbName());
    partition.setTableName(hiveView.getTableName());
    partition.setValues(values);
    partition.setSd(new StorageDescriptor(hiveView.getSd()));
    partition.getSd().setLocation(null);
    return partition;
  }

  public static int getAvailablePort() {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    } catch (IOException e) {
      throw new RuntimeException("Unable to find an available port", e);
    }
  }

  public static URI toUri(File baseLocation, String database, String table) {
    return toUri(baseLocation.toURI().toString(), database, table);
  }

  public static URI toUri(String baseLocation, String database, String table) {
    return URI.create(String.format("%s/%s/%s", baseLocation, database, table)).normalize();
  }

  // This is a little hack to list objects in a S3Proxy bucket that must be used until we patch S3Proxy to not to
  // include directory in the list-objects response
  public static List<S3ObjectSummary> listObjects(AmazonS3 client, String bucket) {
    ObjectListing objects = client.listObjects(bucket);
    List<S3ObjectSummary> result = new ArrayList<>(objects.getObjectSummaries().size());
    for (S3ObjectSummary objectSummary : objects.getObjectSummaries()) {
      if (objectSummary.getKey().endsWith("/") || objectSummary.getKey().endsWith("_$folder$")) {
        continue;
      }
      result.add(objectSummary);
    }
    return result;
  }

}
