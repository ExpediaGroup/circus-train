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
package com.hotels.bdp.circustrain.comparator.hive.comparator;

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import static com.hotels.bdp.circustrain.comparator.TestUtils.newDiff;
import static com.hotels.bdp.circustrain.comparator.TestUtils.newPartition;
import static com.hotels.bdp.circustrain.comparator.TestUtils.newPartitionAndMetadata;
import static com.hotels.bdp.circustrain.comparator.TestUtils.newPropertyDiff;
import static com.hotels.bdp.circustrain.comparator.TestUtils.setCircusTrainSourceParameters;
import static com.hotels.bdp.circustrain.comparator.api.ComparatorType.FULL_COMPARISON;
import static com.hotels.bdp.circustrain.comparator.api.ComparatorType.SHORT_CIRCUIT;

import java.util.List;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet;
import org.apache.hadoop.hive.metastore.api.PrivilegeGrantInfo;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import com.hotels.bdp.circustrain.comparator.ComparatorRegistry;
import com.hotels.bdp.circustrain.comparator.api.ComparatorType;
import com.hotels.bdp.circustrain.comparator.api.Diff;
import com.hotels.bdp.circustrain.comparator.hive.wrappers.PartitionAndMetadata;

public class PartitionAndMetadataComparatorTest {

  private final PartitionAndMetadata left = newPartitionAndMetadata("db", "table", "val");
  private final PartitionAndMetadata right = newPartitionAndMetadata("db", "table", "val");

  private static PartitionAndMetadata replicaPartitionAndMetadata(
      PartitionAndMetadata source,
      String tableName,
      String partitionKey) {
    Partition replica = newPartition(source.getPartition().getDbName(), tableName, partitionKey);
    setCircusTrainSourceParameters(source.getPartition(), replica);
    return new PartitionAndMetadata(source.getSourceTable(), source.getSourceLocation(), replica);
  }

  @Test
  public void equalShortCircuit() {
    List<Diff<Object, Object>> diffs = newPartitionAndMetadataComparator(SHORT_CIRCUIT).compare(left, right);
    assertThat(diffs, is(notNullValue()));
    assertThat(diffs.size(), is(0));
  }

  @Test
  public void equalFullComparison() {
    List<Diff<Object, Object>> diffs = newPartitionAndMetadataComparator(FULL_COMPARISON).compare(left, right);
    assertThat(diffs, is(notNullValue()));
    assertThat(diffs.size(), is(0));
  }

  @Test
  public void differentSourceTableShortCircuit() {
    PartitionAndMetadata source = newPartitionAndMetadata("db", "source", "val");
    PartitionAndMetadata replica = replicaPartitionAndMetadata(newPartitionAndMetadata("db", "other", "val"), "replica",
        "val");
    List<Diff<Object, Object>> diffs = newPartitionAndMetadataComparator(SHORT_CIRCUIT).compare(source, replica);
    assertThat(diffs, is(notNullValue()));
    assertThat(diffs.size(), is(1));
    assertThat(diffs.get(0), is(
        newPropertyDiff(PartitionAndMetadata.class, "sourceTable", source.getSourceTable(), replica.getSourceTable())));
  }

  @Test
  public void differentSourceTableFullComparison() {
    PartitionAndMetadata source = newPartitionAndMetadata("db", "source", "val");
    PartitionAndMetadata replica = replicaPartitionAndMetadata(newPartitionAndMetadata("db", "other", "val"), "replica",
        "val");
    List<Diff<Object, Object>> diffs = newPartitionAndMetadataComparator(FULL_COMPARISON).compare(source, replica);
    assertThat(diffs, is(notNullValue()));
    assertThat(diffs.size(), is(3));
    assertThat(diffs.get(0), is(
        newPropertyDiff(PartitionAndMetadata.class, "sourceTable", source.getSourceTable(), replica.getSourceTable())));
    assertThat(diffs.get(1), is(newPropertyDiff(PartitionAndMetadata.class, "sourceLocation",
        source.getSourceLocation(), replica.getSourceLocation())));
    assertThat(diffs.get(2), is(newPropertyDiff(PartitionAndMetadata.class, "partition.parameters",
        source.getPartition().getParameters(), replica.getPartition().getParameters())));
  }

  @Test
  public void equalShortCircuitWithCircusTrainParameters() {
    PartitionAndMetadata source = newPartitionAndMetadata("db", "source", "val");
    PartitionAndMetadata replica = replicaPartitionAndMetadata(source, "replica", "val");
    List<Diff<Object, Object>> diffs = newPartitionAndMetadataComparator(SHORT_CIRCUIT).compare(source, replica);
    assertThat(diffs, is(notNullValue()));
    assertThat(diffs.size(), is(1));
    assertThat(diffs.get(0), is(newPropertyDiff(PartitionAndMetadata.class, "partition.parameters",
        source.getPartition().getParameters(), replica.getPartition().getParameters())));
  }

  @Test
  public void equalFullComparisonWithCircusTrainParameters() {
    PartitionAndMetadata source = newPartitionAndMetadata("db", "source", "val");
    PartitionAndMetadata replica = replicaPartitionAndMetadata(source, "replica", "val");
    List<Diff<Object, Object>> diffs = newPartitionAndMetadataComparator(FULL_COMPARISON).compare(source, replica);
    assertThat(diffs, is(notNullValue()));
    assertThat(diffs.size(), is(1));
    assertThat(diffs.get(0), is(newPropertyDiff(PartitionAndMetadata.class, "partition.parameters",
        source.getPartition().getParameters(), replica.getPartition().getParameters())));
  }

  @Test
  public void parametersShortCircuit() {
    left.getPartition().getParameters().put("com.company.key", "value");
    List<Diff<Object, Object>> diffs = newPartitionAndMetadataComparator(SHORT_CIRCUIT).compare(left, right);
    assertThat(diffs, is(notNullValue()));
    assertThat(diffs.size(), is(1));
    assertThat(diffs.get(0), is(newPropertyDiff(PartitionAndMetadata.class, "partition.parameters",
        left.getPartition().getParameters(), right.getPartition().getParameters())));
  }

  @Test
  public void parametersFullComparison() {
    left.getPartition().getParameters().put("com.company.key", "value");
    List<Diff<Object, Object>> diffs = newPartitionAndMetadataComparator(FULL_COMPARISON).compare(left, right);
    assertThat(diffs, is(notNullValue()));
    assertThat(diffs.size(), is(1));
    assertThat(diffs.get(0), is(newPropertyDiff(PartitionAndMetadata.class, "partition.parameters",
        left.getPartition().getParameters(), right.getPartition().getParameters())));
  }

  @Test
  public void privilegesShortCircuit() {
    List<PrivilegeGrantInfo> privilege = ImmutableList.of(new PrivilegeGrantInfo());
    left.getPartition().setPrivileges(new PrincipalPrivilegeSet(ImmutableMap.of("write", privilege), null, null));
    List<Diff<Object, Object>> diffs = newPartitionAndMetadataComparator(SHORT_CIRCUIT).compare(left, right);
    assertThat(diffs, is(notNullValue()));
    assertThat(diffs.size(), is(0));
  }

  @Test
  public void privilegesFullComparison() {
    List<PrivilegeGrantInfo> privilege = ImmutableList.of(new PrivilegeGrantInfo());
    left.getPartition().setPrivileges(new PrincipalPrivilegeSet(ImmutableMap.of("write", privilege), null, null));
    List<Diff<Object, Object>> diffs = newPartitionAndMetadataComparator(FULL_COMPARISON).compare(left, right);
    assertThat(diffs, is(notNullValue()));
    assertThat(diffs.size(), is(0));
  }

  @Test
  public void sdLocationShortCircuit() {
    left.getPartition().getSd().setLocation("left");
    List<Diff<Object, Object>> diffs = newPartitionAndMetadataComparator(SHORT_CIRCUIT).compare(left, right);
    assertThat(diffs, is(notNullValue()));
    assertThat(diffs.size(), is(0));
  }

  @Test
  public void sdLocationFullComparison() {
    left.getPartition().getSd().setLocation("left");
    List<Diff<Object, Object>> diffs = newPartitionAndMetadataComparator(FULL_COMPARISON).compare(left, right);
    assertThat(diffs, is(notNullValue()));
    assertThat(diffs.size(), is(0));
  }

  @Test
  public void sdInputFormatShortCircuit() {
    left.getPartition().getSd().setInputFormat("LeftInputFormat");
    List<Diff<Object, Object>> diffs = newPartitionAndMetadataComparator(SHORT_CIRCUIT).compare(left, right);
    assertThat(diffs, is(notNullValue()));
    assertThat(diffs.size(), is(1));
    assertThat(diffs.get(0), is(newPropertyDiff(PartitionAndMetadata.class, "partition.sd.inputFormat",
        left.getPartition().getSd().getInputFormat(), right.getPartition().getSd().getInputFormat())));
  }

  @Test
  public void sdInputFormatFullComparison() {
    left.getPartition().getSd().setInputFormat("LeftInputFormat");
    List<Diff<Object, Object>> diffs = newPartitionAndMetadataComparator(FULL_COMPARISON).compare(left, right);
    assertThat(diffs, is(notNullValue()));
    assertThat(diffs.size(), is(1));
    assertThat(diffs.get(0), is(newPropertyDiff(PartitionAndMetadata.class, "partition.sd.inputFormat",
        left.getPartition().getSd().getInputFormat(), right.getPartition().getSd().getInputFormat())));
  }

  @Test
  public void sdOutputFormatShortCircuit() {
    left.getPartition().getSd().setOutputFormat("LeftOutputFormat");
    List<Diff<Object, Object>> diffs = newPartitionAndMetadataComparator(SHORT_CIRCUIT).compare(left, right);
    assertThat(diffs, is(notNullValue()));
    assertThat(diffs.size(), is(1));
    assertThat(diffs.get(0), is(newPropertyDiff(PartitionAndMetadata.class, "partition.sd.outputFormat",
        left.getPartition().getSd().getOutputFormat(), right.getPartition().getSd().getOutputFormat())));
  }

  @Test
  public void sdOutputFormatFullComparison() {
    left.getPartition().getSd().setOutputFormat("LeftOutputFormat");
    List<Diff<Object, Object>> diffs = newPartitionAndMetadataComparator(FULL_COMPARISON).compare(left, right);
    assertThat(diffs, is(notNullValue()));
    assertThat(diffs.size(), is(1));
    assertThat(diffs.get(0), is(newPropertyDiff(PartitionAndMetadata.class, "partition.sd.outputFormat",
        left.getPartition().getSd().getOutputFormat(), right.getPartition().getSd().getOutputFormat())));
  }

  @Test
  public void sdParametersShortCircuit() {
    left.getPartition().getSd().getParameters().put("com.company.key", "value");
    List<Diff<Object, Object>> diffs = newPartitionAndMetadataComparator(SHORT_CIRCUIT).compare(left, right);
    assertThat(diffs, is(notNullValue()));
    assertThat(diffs.size(), is(1));
    assertThat(diffs.get(0), is(newPropertyDiff(PartitionAndMetadata.class, "partition.sd.parameters",
        left.getPartition().getSd().getParameters(), right.getPartition().getSd().getParameters())));
  }

  @Test
  public void sdParametersFullComparison() {
    left.getPartition().getSd().getParameters().put("com.company.key", "value");
    List<Diff<Object, Object>> diffs = newPartitionAndMetadataComparator(FULL_COMPARISON).compare(left, right);
    assertThat(diffs, is(notNullValue()));
    assertThat(diffs.size(), is(1));
    assertThat(diffs.get(0), is(newPropertyDiff(PartitionAndMetadata.class, "partition.sd.parameters",
        left.getPartition().getSd().getParameters(), right.getPartition().getSd().getParameters())));
  }

  @Test
  public void sdSerdeInfoShortCircuit() {
    left.getPartition().getSd().getSerdeInfo().setName("left serde info");
    List<Diff<Object, Object>> diffs = newPartitionAndMetadataComparator(SHORT_CIRCUIT).compare(left, right);
    assertThat(diffs, is(notNullValue()));
    assertThat(diffs.size(), is(1));
    assertThat(diffs.get(0), is(newPropertyDiff(PartitionAndMetadata.class, "partition.sd.serdeInfo",
        left.getPartition().getSd().getSerdeInfo(), right.getPartition().getSd().getSerdeInfo())));
  }

  @Test
  public void sdSerdeInfoFullComparison() {
    left.getPartition().getSd().getSerdeInfo().setName("left serde info");
    List<Diff<Object, Object>> diffs = newPartitionAndMetadataComparator(FULL_COMPARISON).compare(left, right);
    assertThat(diffs, is(notNullValue()));
    assertThat(diffs.size(), is(1));
    assertThat(diffs.get(0), is(newPropertyDiff(PartitionAndMetadata.class, "partition.sd.serdeInfo",
        left.getPartition().getSd().getSerdeInfo(), right.getPartition().getSd().getSerdeInfo())));
  }

  @Test
  public void sdSkewedInfoShortCircuit() {
    left.getPartition().getSd().getSkewedInfo().setSkewedColNames(ImmutableList.of("left skewed col"));
    List<Diff<Object, Object>> diffs = newPartitionAndMetadataComparator(SHORT_CIRCUIT).compare(left, right);
    assertThat(diffs, is(notNullValue()));
    assertThat(diffs.size(), is(1));
    assertThat(diffs.get(0), is(newPropertyDiff(PartitionAndMetadata.class, "partition.sd.skewedInfo",
        left.getPartition().getSd().getSkewedInfo(), right.getPartition().getSd().getSkewedInfo())));
  }

  @Test
  public void sdSkewedInfoFullComparison() {
    left.getPartition().getSd().getSkewedInfo().setSkewedColNames(ImmutableList.of("left skewed col"));
    List<Diff<Object, Object>> diffs = newPartitionAndMetadataComparator(FULL_COMPARISON).compare(left, right);
    assertThat(diffs, is(notNullValue()));
    assertThat(diffs.size(), is(1));
    assertThat(diffs.get(0), is(newPropertyDiff(PartitionAndMetadata.class, "partition.sd.skewedInfo",
        left.getPartition().getSd().getSkewedInfo(), right.getPartition().getSd().getSkewedInfo())));
  }

  @Test
  public void sdColsShortCircuit() {
    left.getPartition().getSd().setCols(ImmutableList.of(new FieldSchema("left", "type", "comment")));
    List<Diff<Object, Object>> diffs = newPartitionAndMetadataComparator(SHORT_CIRCUIT).compare(left, right);
    assertThat(diffs, is(notNullValue()));
    assertThat(diffs.size(), is(1));
    assertThat(diffs.get(0),
        is(newDiff(
            "Collection partition.sd.cols of class com.google.common.collect.SingletonImmutableList has different size: left.size()=1 and right.size()=2",
            left.getPartition().getSd().getCols(), right.getPartition().getSd().getCols())));
  }

  @Test
  public void sdColsFullComparison() {
    left.getPartition().getSd().setCols(ImmutableList.of(new FieldSchema("left", "type", "comment")));
    List<Diff<Object, Object>> diffs = newPartitionAndMetadataComparator(FULL_COMPARISON).compare(left, right);
    assertThat(diffs, is(notNullValue()));
    assertThat(diffs.size(), is(1));
    assertThat(diffs.get(0),
        is(newDiff(
            "Collection partition.sd.cols of class com.google.common.collect.SingletonImmutableList has different size: left.size()=1 and right.size()=2",
            left.getPartition().getSd().getCols(), right.getPartition().getSd().getCols())));
  }

  @Test
  public void sdColsSameNumberOfElementsShortCircuit() {
    left.getPartition().getSd().setCols(
        ImmutableList.of(new FieldSchema("left1", "type", "comment1"), new FieldSchema("left2", "type", "comment2")));
    List<Diff<Object, Object>> diffs = newPartitionAndMetadataComparator(SHORT_CIRCUIT).compare(left, right);
    assertThat(diffs, is(notNullValue()));
    assertThat(diffs.size(), is(1));
    assertThat(diffs.get(0),
        is(newDiff(
            "Element 0 of collection partition.sd.cols of class com.google.common.collect.RegularImmutableList is different: Property name of class org.apache.hadoop.hive.metastore.api.FieldSchema is different",
            left.getPartition().getSd().getCols().get(0).getName(),
            right.getPartition().getSd().getCols().get(0).getName())));
  }

  @Test
  public void sdColsSameNumberOfElementsFullComparison() {
    left.getPartition().getSd().setCols(
        ImmutableList.of(new FieldSchema("left1", "type", "comment1"), new FieldSchema("left2", "type", "comment2")));
    List<Diff<Object, Object>> diffs = newPartitionAndMetadataComparator(FULL_COMPARISON).compare(left, right);
    assertThat(diffs, is(notNullValue()));
    assertThat(diffs.size(), is(4));
    assertThat(diffs.get(0),
        is(newDiff(
            "Element 0 of collection partition.sd.cols of class com.google.common.collect.RegularImmutableList is different: Property name of class org.apache.hadoop.hive.metastore.api.FieldSchema is different",
            left.getPartition().getSd().getCols().get(0).getName(),
            right.getPartition().getSd().getCols().get(0).getName())));
    assertThat(diffs.get(1),
        is(newDiff(
            "Element 0 of collection partition.sd.cols of class com.google.common.collect.RegularImmutableList is different: Property type of class org.apache.hadoop.hive.metastore.api.FieldSchema is different",
            left.getPartition().getSd().getCols().get(0).getType(),
            right.getPartition().getSd().getCols().get(0).getType())));
    assertThat(diffs.get(2),
        is(newDiff(
            "Element 1 of collection partition.sd.cols of class com.google.common.collect.RegularImmutableList is different: Property name of class org.apache.hadoop.hive.metastore.api.FieldSchema is different",
            left.getPartition().getSd().getCols().get(1).getName(),
            right.getPartition().getSd().getCols().get(1).getName())));
    assertThat(diffs.get(3),
        is(newDiff(
            "Element 1 of collection partition.sd.cols of class com.google.common.collect.RegularImmutableList is different: Property type of class org.apache.hadoop.hive.metastore.api.FieldSchema is different",
            left.getPartition().getSd().getCols().get(1).getType(),
            right.getPartition().getSd().getCols().get(1).getType())));
  }

  @Test
  public void sdSortColsShortCircuit() {
    left.getPartition().getSd().setSortCols(ImmutableList.of(new Order()));
    List<Diff<Object, Object>> diffs = newPartitionAndMetadataComparator(SHORT_CIRCUIT).compare(left, right);
    assertThat(diffs, is(notNullValue()));
    assertThat(diffs.size(), is(1));
    assertThat(diffs.get(0), is(newPropertyDiff(PartitionAndMetadata.class, "partition.sd.sortCols",
        left.getPartition().getSd().getSortCols(), right.getPartition().getSd().getSortCols())));
  }

  @Test
  public void sdSortColsFullComparison() {
    left.getPartition().getSd().setSortCols(ImmutableList.of(new Order()));
    List<Diff<Object, Object>> diffs = newPartitionAndMetadataComparator(FULL_COMPARISON).compare(left, right);
    assertThat(diffs, is(notNullValue()));
    assertThat(diffs.size(), is(1));
    assertThat(diffs.get(0), is(newPropertyDiff(PartitionAndMetadata.class, "partition.sd.sortCols",
        left.getPartition().getSd().getSortCols(), right.getPartition().getSd().getSortCols())));
  }

  @Test
  public void sdBucketColsShortCircuit() {
    left.getPartition().getSd().setBucketCols(ImmutableList.of("bucket"));
    List<Diff<Object, Object>> diffs = newPartitionAndMetadataComparator(SHORT_CIRCUIT).compare(left, right);
    assertThat(diffs, is(notNullValue()));
    assertThat(diffs.size(), is(1));
    assertThat(diffs.get(0), is(newPropertyDiff(PartitionAndMetadata.class, "partition.sd.bucketCols",
        left.getPartition().getSd().getBucketCols(), right.getPartition().getSd().getBucketCols())));
  }

  @Test
  public void sdBucketColsFullComparison() {
    left.getPartition().getSd().setBucketCols(ImmutableList.of("bucket"));
    List<Diff<Object, Object>> diffs = newPartitionAndMetadataComparator(FULL_COMPARISON).compare(left, right);
    assertThat(diffs, is(notNullValue()));
    assertThat(diffs.size(), is(1));
    assertThat(diffs.get(0), is(newPropertyDiff(PartitionAndMetadata.class, "partition.sd.bucketCols",
        left.getPartition().getSd().getBucketCols(), right.getPartition().getSd().getBucketCols())));
  }

  @Test
  public void sdNumBucketsShortCircuit() {
    left.getPartition().getSd().setNumBuckets(9000);
    List<Diff<Object, Object>> diffs = newPartitionAndMetadataComparator(SHORT_CIRCUIT).compare(left, right);
    assertThat(diffs, is(notNullValue()));
    assertThat(diffs.size(), is(1));
    assertThat(diffs.get(0), is(newPropertyDiff(PartitionAndMetadata.class, "partition.sd.numBuckets",
        left.getPartition().getSd().getNumBuckets(), right.getPartition().getSd().getNumBuckets())));
  }

  @Test
  public void sdNumBucketsFullComparison() {
    left.getPartition().getSd().setNumBuckets(9000);
    List<Diff<Object, Object>> diffs = newPartitionAndMetadataComparator(FULL_COMPARISON).compare(left, right);
    assertThat(diffs, is(notNullValue()));
    assertThat(diffs.size(), is(1));
    assertThat(diffs.get(0), is(newPropertyDiff(PartitionAndMetadata.class, "partition.sd.numBuckets",
        left.getPartition().getSd().getNumBuckets(), right.getPartition().getSd().getNumBuckets())));
  }

  @Test
  public void allShortCircuit() {
    left.getPartition().getParameters().put("com.company.key", "value");
    left.getPartition().setValues(ImmutableList.of("p1", "p2"));
    List<PrivilegeGrantInfo> privilege = ImmutableList.of(new PrivilegeGrantInfo());
    left.getPartition().setPrivileges(new PrincipalPrivilegeSet(ImmutableMap.of("write", privilege), null, null));
    left.getPartition().getSd().setLocation("left");
    left.getPartition().getSd().setInputFormat("LeftInputFormat");
    left.getPartition().getSd().setOutputFormat("LeftOutputFormat");
    left.getPartition().getSd().getParameters().put("com.company.key", "value");
    left.getPartition().getSd().getSerdeInfo().setName("left serde info");
    left.getPartition().getSd().getSkewedInfo().setSkewedColNames(ImmutableList.of("left skewed col"));
    left.getPartition().getSd().setCols(ImmutableList.of(new FieldSchema("left", "type", "comment")));
    left.getPartition().getSd().setSortCols(ImmutableList.of(new Order()));
    left.getPartition().getSd().setBucketCols(ImmutableList.of("bucket"));
    left.getPartition().getSd().setNumBuckets(9000);

    List<Diff<Object, Object>> diffs = newPartitionAndMetadataComparator(SHORT_CIRCUIT).compare(left, right);

    assertThat(diffs, is(notNullValue()));
    assertThat(diffs.size(), is(1));
    assertThat(diffs.get(0), is(newPropertyDiff(PartitionAndMetadata.class, "partition.parameters",
        left.getPartition().getParameters(), right.getPartition().getParameters())));
  }

  @Test
  public void allFullComparison() {
    left.getPartition().getParameters().put("com.company.key", "value");
    left.getPartition().setValues(ImmutableList.of("p1", "p2"));
    List<PrivilegeGrantInfo> privilege = ImmutableList.of(new PrivilegeGrantInfo());
    left.getPartition().setPrivileges(new PrincipalPrivilegeSet(ImmutableMap.of("write", privilege), null, null));
    left.getPartition().getSd().setLocation("left");
    left.getPartition().getSd().setInputFormat("LeftInputFormat");
    left.getPartition().getSd().setOutputFormat("LeftOutputFormat");
    left.getPartition().getSd().getParameters().put("com.company.key", "value");
    left.getPartition().getSd().getSerdeInfo().setName("left serde info");
    left.getPartition().getSd().getSkewedInfo().setSkewedColNames(ImmutableList.of("left skewed col"));
    left.getPartition().getSd().setCols(ImmutableList.of(new FieldSchema("left", "type", "comment")));
    left.getPartition().getSd().setSortCols(ImmutableList.of(new Order()));
    left.getPartition().getSd().setBucketCols(ImmutableList.of("bucket"));
    left.getPartition().getSd().setNumBuckets(9000);

    List<Diff<Object, Object>> diffs = newPartitionAndMetadataComparator(FULL_COMPARISON).compare(left, right);

    assertThat(diffs, is(notNullValue()));
    assertThat(diffs.size(), is(10));
    assertThat(diffs.get(0), is(newPropertyDiff(PartitionAndMetadata.class, "partition.parameters",
        left.getPartition().getParameters(), right.getPartition().getParameters())));
    assertThat(diffs.get(1), is(newPropertyDiff(PartitionAndMetadata.class, "partition.sd.inputFormat",
        left.getPartition().getSd().getInputFormat(), right.getPartition().getSd().getInputFormat())));
    assertThat(diffs.get(2), is(newPropertyDiff(PartitionAndMetadata.class, "partition.sd.outputFormat",
        left.getPartition().getSd().getOutputFormat(), right.getPartition().getSd().getOutputFormat())));
    assertThat(diffs.get(3), is(newPropertyDiff(PartitionAndMetadata.class, "partition.sd.parameters",
        left.getPartition().getSd().getParameters(), right.getPartition().getSd().getParameters())));
    assertThat(diffs.get(4), is(newPropertyDiff(PartitionAndMetadata.class, "partition.sd.serdeInfo",
        left.getPartition().getSd().getSerdeInfo(), right.getPartition().getSd().getSerdeInfo())));
    assertThat(diffs.get(5), is(newPropertyDiff(PartitionAndMetadata.class, "partition.sd.skewedInfo",
        left.getPartition().getSd().getSkewedInfo(), right.getPartition().getSd().getSkewedInfo())));
    assertThat(diffs.get(6),
        is(newDiff(
            "Collection partition.sd.cols of class com.google.common.collect.SingletonImmutableList has different size: left.size()=1 and right.size()=2",
            left.getPartition().getSd().getCols(), right.getPartition().getSd().getCols())));
    assertThat(diffs.get(7), is(newPropertyDiff(PartitionAndMetadata.class, "partition.sd.sortCols",
        left.getPartition().getSd().getSortCols(), right.getPartition().getSd().getSortCols())));
    assertThat(diffs.get(8), is(newPropertyDiff(PartitionAndMetadata.class, "partition.sd.bucketCols",
        left.getPartition().getSd().getBucketCols(), right.getPartition().getSd().getBucketCols())));
    assertThat(diffs.get(9), is(newPropertyDiff(PartitionAndMetadata.class, "partition.sd.numBuckets",
        left.getPartition().getSd().getNumBuckets(), right.getPartition().getSd().getNumBuckets())));
  }

  private PartitionAndMetadataComparator newPartitionAndMetadataComparator(ComparatorType comparatorType) {
    ComparatorRegistry comparatorRegistry = new ComparatorRegistry(comparatorType);
    return new PartitionAndMetadataComparator(comparatorRegistry, comparatorType);
  }
}
