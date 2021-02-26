/**
 * Copyright (C) 2016-2021 Expedia, Inc.
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
package com.hotels.bdp.circustrain.comparator.hive.comparator;

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import static com.hotels.bdp.circustrain.comparator.TestUtils.newDiff;
import static com.hotels.bdp.circustrain.comparator.TestUtils.newPropertyDiff;
import static com.hotels.bdp.circustrain.comparator.TestUtils.newTable;
import static com.hotels.bdp.circustrain.comparator.TestUtils.newTableAndMetadata;
import static com.hotels.bdp.circustrain.comparator.TestUtils.setCircusTrainSourceParameters;
import static com.hotels.bdp.circustrain.comparator.api.ComparatorType.FULL_COMPARISON;
import static com.hotels.bdp.circustrain.comparator.api.ComparatorType.SHORT_CIRCUIT;

import java.util.List;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet;
import org.apache.hadoop.hive.metastore.api.PrivilegeGrantInfo;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import com.hotels.bdp.circustrain.comparator.ComparatorRegistry;
import com.hotels.bdp.circustrain.comparator.api.ComparatorType;
import com.hotels.bdp.circustrain.comparator.api.Diff;
import com.hotels.bdp.circustrain.comparator.hive.wrappers.TableAndMetadata;

public class TableAndMetadataComparatorTest {

  private final TableAndMetadata left = newTableAndMetadata("db", "table");
  private final TableAndMetadata right = newTableAndMetadata("db", "table");

  private static TableAndMetadata replicaTableAndMetadata(TableAndMetadata source, String tableName) {
    Table replica = newTable(source.getTable().getDbName(), tableName);
    setCircusTrainSourceParameters(source.getTable(), replica);
    return new TableAndMetadata(source.getSourceTable(), source.getSourceLocation(), replica);
  }

  @Test
  public void equalShortCircuit() {
    List<Diff<Object, Object>> diffs = newTableAndMetadataComparator(SHORT_CIRCUIT).compare(left, right);
    assertThat(diffs, is(notNullValue()));
    assertThat(diffs.size(), is(0));
  }

  @Test
  public void equalFullComparison() {
    List<Diff<Object, Object>> diffs = newTableAndMetadataComparator(FULL_COMPARISON).compare(left, right);
    assertThat(diffs, is(notNullValue()));
    assertThat(diffs.size(), is(0));
  }

  @Test
  public void differentSourceTableShortCircuit() {
    TableAndMetadata source = newTableAndMetadata("db", "source");
    TableAndMetadata replica = replicaTableAndMetadata(newTableAndMetadata("db", "other"), "replica");
    List<Diff<Object, Object>> diffs = newTableAndMetadataComparator(SHORT_CIRCUIT).compare(source, replica);
    assertThat(diffs, is(notNullValue()));
    assertThat(diffs.size(), is(1));
    assertThat(diffs.get(0),
        is(newPropertyDiff(TableAndMetadata.class, "sourceTable", source.getSourceTable(), replica.getSourceTable())));
  }

  @Test
  public void differentSourceTableFullComparison() {
    TableAndMetadata source = newTableAndMetadata("db", "source");
    TableAndMetadata replica = replicaTableAndMetadata(newTableAndMetadata("db", "other"), "replica");
    List<Diff<Object, Object>> diffs = newTableAndMetadataComparator(FULL_COMPARISON).compare(source, replica);
    assertThat(diffs, is(notNullValue()));
    assertThat(diffs.size(), is(3));
    assertThat(diffs.get(0),
        is(newPropertyDiff(TableAndMetadata.class, "sourceTable", source.getSourceTable(), replica.getSourceTable())));
    assertThat(diffs.get(1), is(newPropertyDiff(TableAndMetadata.class, "sourceLocation", source.getSourceLocation(),
        replica.getSourceLocation())));
    assertThat(diffs.get(2), is(newPropertyDiff(TableAndMetadata.class, "table.parameters",
        source.getTable().getParameters(), replica.getTable().getParameters())));
  }

  @Test
  public void equalShortCircuitWithCircusTrainParameters() {
    TableAndMetadata source = newTableAndMetadata("db", "source");
    TableAndMetadata replica = replicaTableAndMetadata(source, "replica");
    List<Diff<Object, Object>> diffs = newTableAndMetadataComparator(SHORT_CIRCUIT).compare(source, replica);
    assertThat(diffs, is(notNullValue()));
    assertThat(diffs.size(), is(1));
    assertThat(diffs.get(0), is(newPropertyDiff(TableAndMetadata.class, "table.parameters",
        source.getTable().getParameters(), replica.getTable().getParameters())));
  }

  @Test
  public void equalFullComparisonWithCircusTrainParameters() {
    TableAndMetadata source = newTableAndMetadata("db", "source");
    TableAndMetadata replica = replicaTableAndMetadata(source, "replica");
    List<Diff<Object, Object>> diffs = newTableAndMetadataComparator(FULL_COMPARISON).compare(source, replica);
    assertThat(diffs, is(notNullValue()));
    assertThat(diffs.size(), is(1));
    assertThat(diffs.get(0), is(newPropertyDiff(TableAndMetadata.class, "table.parameters",
        source.getTable().getParameters(), replica.getTable().getParameters())));
  }

  @Test
  public void parametersShortCircuit() {
    left.getTable().getParameters().put("com.company.key", "value");
    List<Diff<Object, Object>> diffs = newTableAndMetadataComparator(SHORT_CIRCUIT).compare(left, right);
    assertThat(diffs, is(notNullValue()));
    assertThat(diffs.size(), is(1));
    assertThat(diffs.get(0), is(newPropertyDiff(TableAndMetadata.class, "table.parameters",
        left.getTable().getParameters(), right.getTable().getParameters())));
  }

  @Test
  public void parametersFullComparison() {
    left.getTable().getParameters().put("com.company.key", "value");
    List<Diff<Object, Object>> diffs = newTableAndMetadataComparator(FULL_COMPARISON).compare(left, right);
    assertThat(diffs, is(notNullValue()));
    assertThat(diffs.size(), is(1));
    assertThat(diffs.get(0), is(newPropertyDiff(TableAndMetadata.class, "table.parameters",
        left.getTable().getParameters(), right.getTable().getParameters())));
  }

  @Test
  public void partitionKeysShortCircuit() {
    left.getTable().setPartitionKeys(ImmutableList.of(new FieldSchema("p", "string", "p comment")));
    List<Diff<Object, Object>> diffs = newTableAndMetadataComparator(SHORT_CIRCUIT).compare(left, right);
    assertThat(diffs, is(notNullValue()));
    assertThat(diffs.size(), is(1));
    assertThat(diffs.get(0), is(newPropertyDiff(TableAndMetadata.class, "table.partitionKeys",
        left.getTable().getPartitionKeys(), right.getTable().getPartitionKeys())));
  }

  @Test
  public void partitionKeysFullComparison() {
    left.getTable().setPartitionKeys(ImmutableList.of(new FieldSchema("p", "string", "p comment")));
    List<Diff<Object, Object>> diffs = newTableAndMetadataComparator(FULL_COMPARISON).compare(left, right);
    assertThat(diffs, is(notNullValue()));
    assertThat(diffs.size(), is(1));
    assertThat(diffs.get(0), is(newPropertyDiff(TableAndMetadata.class, "table.partitionKeys",
        left.getTable().getPartitionKeys(), right.getTable().getPartitionKeys())));
  }

  @Test
  public void ownerShortCircuit() {
    left.getTable().setOwner("left owner");
    List<Diff<Object, Object>> diffs = newTableAndMetadataComparator(SHORT_CIRCUIT).compare(left, right);
    assertThat(diffs, is(notNullValue()));
    assertThat(diffs.size(), is(0));
  }

  @Test
  public void ownerFullComparison() {
    left.getTable().setOwner("left owner");
    List<Diff<Object, Object>> diffs = newTableAndMetadataComparator(FULL_COMPARISON).compare(left, right);
    assertThat(diffs, is(notNullValue()));
    assertThat(diffs.size(), is(0));
  }

  @Test
  public void privilegesShortCircuit() {
    List<PrivilegeGrantInfo> privilege = ImmutableList.of(new PrivilegeGrantInfo());
    left.getTable().setPrivileges(new PrincipalPrivilegeSet(ImmutableMap.of("write", privilege), null, null));
    List<Diff<Object, Object>> diffs = newTableAndMetadataComparator(SHORT_CIRCUIT).compare(left, right);
    assertThat(diffs, is(notNullValue()));
    assertThat(diffs.size(), is(0));
  }

  @Test
  public void privilegesFullComparison() {
    List<PrivilegeGrantInfo> privilege = ImmutableList.of(new PrivilegeGrantInfo());
    left.getTable().setPrivileges(new PrincipalPrivilegeSet(ImmutableMap.of("write", privilege), null, null));
    List<Diff<Object, Object>> diffs = newTableAndMetadataComparator(FULL_COMPARISON).compare(left, right);
    assertThat(diffs, is(notNullValue()));
    assertThat(diffs.size(), is(0));
  }

  @Test
  public void retentionShortCircuit() {
    left.getTable().setRetention(2);
    List<Diff<Object, Object>> diffs = newTableAndMetadataComparator(SHORT_CIRCUIT).compare(left, right);
    assertThat(diffs, is(notNullValue()));
    assertThat(diffs.size(), is(1));
    assertThat(diffs.get(0), is(newPropertyDiff(TableAndMetadata.class, "table.retention",
        left.getTable().getRetention(), right.getTable().getRetention())));
  }

  @Test
  public void retentionFullComparison() {
    left.getTable().setRetention(2);
    List<Diff<Object, Object>> diffs = newTableAndMetadataComparator(FULL_COMPARISON).compare(left, right);
    assertThat(diffs, is(notNullValue()));
    assertThat(diffs.size(), is(1));
    assertThat(diffs.get(0), is(newPropertyDiff(TableAndMetadata.class, "table.retention",
        left.getTable().getRetention(), right.getTable().getRetention())));
  }

  @Test
  public void tableTypeShortCircuit() {
    // we always set the replica table to be EXTERNAL so we can ignore the tableType
    left.getTable().setTableType("internal");
    List<Diff<Object, Object>> diffs = newTableAndMetadataComparator(SHORT_CIRCUIT).compare(left, right);
    assertThat(diffs, is(notNullValue()));
    assertThat(diffs.size(), is(0));
  }

  @Test
  public void tableTypeFullComparison() {
    // we always set the replica table to be EXTERNAL so we can ignore the tableType
    left.getTable().setTableType("internal");
    List<Diff<Object, Object>> diffs = newTableAndMetadataComparator(FULL_COMPARISON).compare(left, right);
    assertThat(diffs, is(notNullValue()));
    assertThat(diffs.size(), is(0));
  }

  @Test
  public void sdLocationShortCircuit() {
    left.getTable().getSd().setLocation("left");
    List<Diff<Object, Object>> diffs = newTableAndMetadataComparator(SHORT_CIRCUIT).compare(left, right);
    assertThat(diffs, is(notNullValue()));
    assertThat(diffs.size(), is(0));
  }

  @Test
  public void sdLocationFullComparison() {
    left.getTable().getSd().setLocation("left");
    List<Diff<Object, Object>> diffs = newTableAndMetadataComparator(FULL_COMPARISON).compare(left, right);
    assertThat(diffs, is(notNullValue()));
    assertThat(diffs.size(), is(0));
  }

  @Test
  public void sdInputFormatShortCircuit() {
    left.getTable().getSd().setInputFormat("LeftInputFormat");
    List<Diff<Object, Object>> diffs = newTableAndMetadataComparator(SHORT_CIRCUIT).compare(left, right);
    assertThat(diffs, is(notNullValue()));
    assertThat(diffs.size(), is(1));
    assertThat(diffs.get(0), is(newPropertyDiff(TableAndMetadata.class, "table.sd.inputFormat",
        left.getTable().getSd().getInputFormat(), right.getTable().getSd().getInputFormat())));
  }

  @Test
  public void sdInputFormatFullComparison() {
    left.getTable().getSd().setInputFormat("LeftInputFormat");
    List<Diff<Object, Object>> diffs = newTableAndMetadataComparator(FULL_COMPARISON).compare(left, right);
    assertThat(diffs, is(notNullValue()));
    assertThat(diffs.size(), is(1));
    assertThat(diffs.get(0), is(newPropertyDiff(TableAndMetadata.class, "table.sd.inputFormat",
        left.getTable().getSd().getInputFormat(), right.getTable().getSd().getInputFormat())));
  }

  @Test
  public void sdOutputFormatShortCircuit() {
    left.getTable().getSd().setOutputFormat("LeftOutputFormat");
    List<Diff<Object, Object>> diffs = newTableAndMetadataComparator(SHORT_CIRCUIT).compare(left, right);
    assertThat(diffs, is(notNullValue()));
    assertThat(diffs.size(), is(1));
    assertThat(diffs.get(0), is(newPropertyDiff(TableAndMetadata.class, "table.sd.outputFormat",
        left.getTable().getSd().getOutputFormat(), right.getTable().getSd().getOutputFormat())));
  }

  @Test
  public void sdOutputFormatFullComparison() {
    left.getTable().getSd().setOutputFormat("LeftOutputFormat");
    List<Diff<Object, Object>> diffs = newTableAndMetadataComparator(FULL_COMPARISON).compare(left, right);
    assertThat(diffs, is(notNullValue()));
    assertThat(diffs.size(), is(1));
    assertThat(diffs.get(0), is(newPropertyDiff(TableAndMetadata.class, "table.sd.outputFormat",
        left.getTable().getSd().getOutputFormat(), right.getTable().getSd().getOutputFormat())));
  }

  @Test
  public void sdParametersShortCircuit() {
    left.getTable().getSd().getParameters().put("com.company.key", "value");
    List<Diff<Object, Object>> diffs = newTableAndMetadataComparator(SHORT_CIRCUIT).compare(left, right);
    assertThat(diffs, is(notNullValue()));
    assertThat(diffs.size(), is(1));
    assertThat(diffs.get(0), is(newPropertyDiff(TableAndMetadata.class, "table.sd.parameters",
        left.getTable().getSd().getParameters(), right.getTable().getSd().getParameters())));
  }

  @Test
  public void sdParametersFullComparison() {
    left.getTable().getSd().getParameters().put("com.company.key", "value");
    List<Diff<Object, Object>> diffs = newTableAndMetadataComparator(FULL_COMPARISON).compare(left, right);
    assertThat(diffs, is(notNullValue()));
    assertThat(diffs.size(), is(1));
    assertThat(diffs.get(0), is(newPropertyDiff(TableAndMetadata.class, "table.sd.parameters",
        left.getTable().getSd().getParameters(), right.getTable().getSd().getParameters())));
  }

  @Test
  public void sdSerdeInfoShortCircuit() {
    left.getTable().getSd().getSerdeInfo().setName("left serde info");
    List<Diff<Object, Object>> diffs = newTableAndMetadataComparator(SHORT_CIRCUIT).compare(left, right);
    assertThat(diffs, is(notNullValue()));
    assertThat(diffs.size(), is(1));
    assertThat(diffs.get(0), is(newPropertyDiff(TableAndMetadata.class, "table.sd.serdeInfo",
        left.getTable().getSd().getSerdeInfo(), right.getTable().getSd().getSerdeInfo())));
  }

  @Test
  public void sdSerdeInfoFullComparison() {
    left.getTable().getSd().getSerdeInfo().setName("left serde info");
    List<Diff<Object, Object>> diffs = newTableAndMetadataComparator(FULL_COMPARISON).compare(left, right);
    assertThat(diffs, is(notNullValue()));
    assertThat(diffs.size(), is(1));
    assertThat(diffs.get(0), is(newPropertyDiff(TableAndMetadata.class, "table.sd.serdeInfo",
        left.getTable().getSd().getSerdeInfo(), right.getTable().getSd().getSerdeInfo())));
  }

  @Test
  public void sdSkewedInfoShortCircuit() {
    left.getTable().getSd().getSkewedInfo().setSkewedColNames(ImmutableList.of("left skewed col"));
    List<Diff<Object, Object>> diffs = newTableAndMetadataComparator(SHORT_CIRCUIT).compare(left, right);
    assertThat(diffs, is(notNullValue()));
    assertThat(diffs.size(), is(1));
    assertThat(diffs.get(0), is(newPropertyDiff(TableAndMetadata.class, "table.sd.skewedInfo",
        left.getTable().getSd().getSkewedInfo(), right.getTable().getSd().getSkewedInfo())));
  }

  @Test
  public void sdSkewedInfoFullComparison() {
    left.getTable().getSd().getSkewedInfo().setSkewedColNames(ImmutableList.of("left skewed col"));
    List<Diff<Object, Object>> diffs = newTableAndMetadataComparator(FULL_COMPARISON).compare(left, right);
    assertThat(diffs, is(notNullValue()));
    assertThat(diffs.size(), is(1));
    assertThat(diffs.get(0), is(newPropertyDiff(TableAndMetadata.class, "table.sd.skewedInfo",
        left.getTable().getSd().getSkewedInfo(), right.getTable().getSd().getSkewedInfo())));
  }

  @Test
  public void sdColsShortCircuit() {
    left.getTable().getSd().setCols(ImmutableList.of(new FieldSchema("left", "type", "comment")));
    List<Diff<Object, Object>> diffs = newTableAndMetadataComparator(SHORT_CIRCUIT).compare(left, right);
    assertThat(diffs, is(notNullValue()));
    assertThat(diffs.size(), is(1));
    assertThat(diffs.get(0),
        is(newDiff(
            "Collection table.sd.cols of class com.google.common.collect.SingletonImmutableList has different size: left.size()=1 and right.size()=2",
            left.getTable().getSd().getCols(), right.getTable().getSd().getCols())));
  }

  @Test
  public void sdColsFullComparison() {
    left.getTable().getSd().setCols(ImmutableList.of(new FieldSchema("left", "type", "comment")));
    List<Diff<Object, Object>> diffs = newTableAndMetadataComparator(FULL_COMPARISON).compare(left, right);
    assertThat(diffs, is(notNullValue()));
    assertThat(diffs.size(), is(1));
    assertThat(diffs.get(0),
        is(newDiff(
            "Collection table.sd.cols of class com.google.common.collect.SingletonImmutableList has different size: left.size()=1 and right.size()=2",
            left.getTable().getSd().getCols(), right.getTable().getSd().getCols())));
  }

  @Test
  public void sdSortColsShortCircuit() {
    left.getTable().getSd().setSortCols(ImmutableList.of(new Order()));
    List<Diff<Object, Object>> diffs = newTableAndMetadataComparator(SHORT_CIRCUIT).compare(left, right);
    assertThat(diffs, is(notNullValue()));
    assertThat(diffs.size(), is(1));
    assertThat(diffs.get(0), is(newPropertyDiff(TableAndMetadata.class, "table.sd.sortCols",
        left.getTable().getSd().getSortCols(), right.getTable().getSd().getSortCols())));
  }

  @Test
  public void sdColsSameNumberOfColsShortCircuit() {
    left.getTable().getSd().setCols(
        ImmutableList.of(new FieldSchema("left1", "type", "comment1"), new FieldSchema("left2", "type", "comment2")));
    List<Diff<Object, Object>> diffs = newTableAndMetadataComparator(SHORT_CIRCUIT).compare(left, right);
    assertThat(diffs, is(notNullValue()));
    assertThat(diffs.size(), is(1));
    assertThat(diffs.get(0),
        is(newDiff(
            "Element 0 of collection table.sd.cols of class com.google.common.collect.RegularImmutableList is different: Property name of class org.apache.hadoop.hive.metastore.api.FieldSchema is different",
            left.getTable().getSd().getCols().get(0).getName(), right.getTable().getSd().getCols().get(0).getName())));
  }

  @Test
  public void sdColsSameNumberOfColsFullComparison() {
    left.getTable().getSd().setCols(
        ImmutableList.of(new FieldSchema("left1", "type", "comment1"), new FieldSchema("left2", "type", "comment2")));
    List<Diff<Object, Object>> diffs = newTableAndMetadataComparator(FULL_COMPARISON).compare(left, right);
    assertThat(diffs, is(notNullValue()));
    assertThat(diffs.size(), is(4));
    assertThat(diffs.get(0),
        is(newDiff(
            "Element 0 of collection table.sd.cols of class com.google.common.collect.RegularImmutableList is different: Property name of class org.apache.hadoop.hive.metastore.api.FieldSchema is different",
            left.getTable().getSd().getCols().get(0).getName(), right.getTable().getSd().getCols().get(0).getName())));
    assertThat(diffs.get(1),
        is(newDiff(
            "Element 0 of collection table.sd.cols of class com.google.common.collect.RegularImmutableList is different: Property type of class org.apache.hadoop.hive.metastore.api.FieldSchema is different",
            left.getTable().getSd().getCols().get(0).getType(), right.getTable().getSd().getCols().get(0).getType())));
    assertThat(diffs.get(2),
        is(newDiff(
            "Element 1 of collection table.sd.cols of class com.google.common.collect.RegularImmutableList is different: Property name of class org.apache.hadoop.hive.metastore.api.FieldSchema is different",
            left.getTable().getSd().getCols().get(1).getName(), right.getTable().getSd().getCols().get(1).getName())));
    assertThat(diffs.get(3),
        is(newDiff(
            "Element 1 of collection table.sd.cols of class com.google.common.collect.RegularImmutableList is different: Property type of class org.apache.hadoop.hive.metastore.api.FieldSchema is different",
            left.getTable().getSd().getCols().get(1).getType(), right.getTable().getSd().getCols().get(1).getType())));
  }

  @Test
  public void sdSortColsFullComparison() {
    left.getTable().getSd().setSortCols(ImmutableList.of(new Order()));
    List<Diff<Object, Object>> diffs = newTableAndMetadataComparator(FULL_COMPARISON).compare(left, right);
    assertThat(diffs, is(notNullValue()));
    assertThat(diffs.size(), is(1));
    assertThat(diffs.get(0), is(newPropertyDiff(TableAndMetadata.class, "table.sd.sortCols",
        left.getTable().getSd().getSortCols(), right.getTable().getSd().getSortCols())));
  }

  @Test
  public void sdBucketColsShortCircuit() {
    left.getTable().getSd().setBucketCols(ImmutableList.of("bucket"));
    List<Diff<Object, Object>> diffs = newTableAndMetadataComparator(SHORT_CIRCUIT).compare(left, right);
    assertThat(diffs, is(notNullValue()));
    assertThat(diffs.size(), is(1));
    assertThat(diffs.get(0), is(newPropertyDiff(TableAndMetadata.class, "table.sd.bucketCols",
        left.getTable().getSd().getBucketCols(), right.getTable().getSd().getBucketCols())));
  }

  @Test
  public void sdBucketColsFullComparison() {
    left.getTable().getSd().setBucketCols(ImmutableList.of("bucket"));
    List<Diff<Object, Object>> diffs = newTableAndMetadataComparator(FULL_COMPARISON).compare(left, right);
    assertThat(diffs, is(notNullValue()));
    assertThat(diffs.size(), is(1));
    assertThat(diffs.get(0), is(newPropertyDiff(TableAndMetadata.class, "table.sd.bucketCols",
        left.getTable().getSd().getBucketCols(), right.getTable().getSd().getBucketCols())));
  }

  @Test
  public void sdNumBucketsShortCircuit() {
    left.getTable().getSd().setNumBuckets(9000);
    List<Diff<Object, Object>> diffs = newTableAndMetadataComparator(SHORT_CIRCUIT).compare(left, right);
    assertThat(diffs, is(notNullValue()));
    assertThat(diffs.size(), is(1));
    assertThat(diffs.get(0), is(newPropertyDiff(TableAndMetadata.class, "table.sd.numBuckets",
        left.getTable().getSd().getNumBuckets(), right.getTable().getSd().getNumBuckets())));
  }

  @Test
  public void sdNumBucketsFullComparison() {
    left.getTable().getSd().setNumBuckets(9000);
    List<Diff<Object, Object>> diffs = newTableAndMetadataComparator(FULL_COMPARISON).compare(left, right);
    assertThat(diffs, is(notNullValue()));
    assertThat(diffs.size(), is(1));
    assertThat(diffs.get(0), is(newPropertyDiff(TableAndMetadata.class, "table.sd.numBuckets",
        left.getTable().getSd().getNumBuckets(), right.getTable().getSd().getNumBuckets())));
  }

  @Test
  public void allShortCircuit() {
    left.getTable().getParameters().put("com.company.key", "value");
    left.getTable().setPartitionKeys(ImmutableList.of(new FieldSchema("p", "string", "p comment")));
    left.getTable().setOwner("left owner");
    List<PrivilegeGrantInfo> privilege = ImmutableList.of(new PrivilegeGrantInfo());
    left.getTable().setPrivileges(new PrincipalPrivilegeSet(ImmutableMap.of("write", privilege), null, null));
    left.getTable().setRetention(2);
    left.getTable().setTableType("internal");
    left.getTable().getSd().setLocation("left");
    left.getTable().getSd().setInputFormat("LeftInputFormat");
    left.getTable().getSd().setOutputFormat("LeftOutputFormat");
    left.getTable().getSd().getParameters().put("com.company.key", "value");
    left.getTable().getSd().getSerdeInfo().setName("left serde info");
    left.getTable().getSd().getSkewedInfo().setSkewedColNames(ImmutableList.of("left skewed col"));
    left.getTable().getSd().setCols(ImmutableList.of(new FieldSchema("left", "type", "comment")));
    left.getTable().getSd().setSortCols(ImmutableList.of(new Order()));
    left.getTable().getSd().setBucketCols(ImmutableList.of("bucket"));
    left.getTable().getSd().setNumBuckets(9000);

    List<Diff<Object, Object>> diffs = newTableAndMetadataComparator(SHORT_CIRCUIT).compare(left, right);

    assertThat(diffs, is(notNullValue()));
    assertThat(diffs.size(), is(1));
    assertThat(diffs.get(0), is(newPropertyDiff(TableAndMetadata.class, "table.parameters",
        left.getTable().getParameters(), right.getTable().getParameters())));
  }

  @Test
  public void allFullComparison() {
    left.getTable().getParameters().put("com.company.key", "value");
    left.getTable().setPartitionKeys(ImmutableList.of(new FieldSchema("p", "string", "p comment")));
    left.getTable().setOwner("left owner");
    List<PrivilegeGrantInfo> privilege = ImmutableList.of(new PrivilegeGrantInfo());
    left.getTable().setPrivileges(new PrincipalPrivilegeSet(ImmutableMap.of("write", privilege), null, null));
    left.getTable().setRetention(2);
    left.getTable().setTableType("internal");
    left.getTable().getSd().setLocation("left");
    left.getTable().getSd().setInputFormat("LeftInputFormat");
    left.getTable().getSd().setOutputFormat("LeftOutputFormat");
    left.getTable().getSd().getParameters().put("com.company.key", "value");
    left.getTable().getSd().getSerdeInfo().setName("left serde info");
    left.getTable().getSd().getSkewedInfo().setSkewedColNames(ImmutableList.of("left skewed col"));
    left.getTable().getSd().setCols(ImmutableList.of(new FieldSchema("left", "type", "comment")));
    left.getTable().getSd().setSortCols(ImmutableList.of(new Order()));
    left.getTable().getSd().setBucketCols(ImmutableList.of("bucket"));
    left.getTable().getSd().setNumBuckets(9000);

    List<Diff<Object, Object>> diffs = newTableAndMetadataComparator(FULL_COMPARISON).compare(left, right);

    assertThat(diffs, is(notNullValue()));
    assertThat(diffs.size(), is(12));
    assertThat(diffs.get(0), is(newPropertyDiff(TableAndMetadata.class, "table.parameters",
        left.getTable().getParameters(), right.getTable().getParameters())));
    assertThat(diffs.get(1), is(newPropertyDiff(TableAndMetadata.class, "table.partitionKeys",
        left.getTable().getPartitionKeys(), right.getTable().getPartitionKeys())));
    assertThat(diffs.get(2), is(newPropertyDiff(TableAndMetadata.class, "table.retention",
        left.getTable().getRetention(), right.getTable().getRetention())));
    assertThat(diffs.get(3), is(newPropertyDiff(TableAndMetadata.class, "table.sd.inputFormat",
        left.getTable().getSd().getInputFormat(), right.getTable().getSd().getInputFormat())));
    assertThat(diffs.get(4), is(newPropertyDiff(TableAndMetadata.class, "table.sd.outputFormat",
        left.getTable().getSd().getOutputFormat(), right.getTable().getSd().getOutputFormat())));
    assertThat(diffs.get(5), is(newPropertyDiff(TableAndMetadata.class, "table.sd.parameters",
        left.getTable().getSd().getParameters(), right.getTable().getSd().getParameters())));
    assertThat(diffs.get(6), is(newPropertyDiff(TableAndMetadata.class, "table.sd.serdeInfo",
        left.getTable().getSd().getSerdeInfo(), right.getTable().getSd().getSerdeInfo())));
    assertThat(diffs.get(7), is(newPropertyDiff(TableAndMetadata.class, "table.sd.skewedInfo",
        left.getTable().getSd().getSkewedInfo(), right.getTable().getSd().getSkewedInfo())));
    assertThat(diffs.get(8),
        is(newDiff(
            "Collection table.sd.cols of class com.google.common.collect.SingletonImmutableList has different size: left.size()=1 and right.size()=2",
            left.getTable().getSd().getCols(), right.getTable().getSd().getCols())));
    assertThat(diffs.get(9), is(newPropertyDiff(TableAndMetadata.class, "table.sd.sortCols",
        left.getTable().getSd().getSortCols(), right.getTable().getSd().getSortCols())));
    assertThat(diffs.get(10), is(newPropertyDiff(TableAndMetadata.class, "table.sd.bucketCols",
        left.getTable().getSd().getBucketCols(), right.getTable().getSd().getBucketCols())));
    assertThat(diffs.get(11), is(newPropertyDiff(TableAndMetadata.class, "table.sd.numBuckets",
        left.getTable().getSd().getNumBuckets(), right.getTable().getSd().getNumBuckets())));
  }

  private TableAndMetadataComparator newTableAndMetadataComparator(ComparatorType comparatorType) {
    ComparatorRegistry comparatorRegistry = new ComparatorRegistry(comparatorType);
    return new TableAndMetadataComparator(comparatorRegistry, comparatorType);
  }
}
