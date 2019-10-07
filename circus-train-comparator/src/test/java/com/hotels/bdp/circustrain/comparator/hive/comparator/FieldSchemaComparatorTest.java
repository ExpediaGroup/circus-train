/**
 * Copyright (C) 2016-2019 Expedia, Inc.
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

import static com.hotels.bdp.circustrain.comparator.TestUtils.newPropertyDiff;
import static com.hotels.bdp.circustrain.comparator.api.ComparatorType.FULL_COMPARISON;
import static com.hotels.bdp.circustrain.comparator.api.ComparatorType.SHORT_CIRCUIT;

import java.util.List;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.junit.Before;
import org.junit.Test;

import com.hotels.bdp.circustrain.comparator.api.Diff;

public class FieldSchemaComparatorTest {

  private FieldSchema left;
  private FieldSchema right;

  @Before
  public void init() {
    left = new FieldSchema();
    left.setName("name");
    left.setType("string");
    left.setComment("comment");

    right = new FieldSchema();
    right.setName(left.getName());
    right.setType(left.getType());
    right.setComment(left.getComment());
  }

  @Test
  public void equalShortCircuit() {
    List<Diff<Object, Object>> diffs = new FieldSchemaComparator(SHORT_CIRCUIT).compare(left, right);
    assertThat(diffs, is(notNullValue()));
    assertThat(diffs.size(), is(0));
  }

  @Test
  public void equalFullComparison() {
    List<Diff<Object, Object>> diffs = new FieldSchemaComparator(FULL_COMPARISON).compare(left, right);
    assertThat(diffs, is(notNullValue()));
    assertThat(diffs.size(), is(0));
  }

  @Test
  public void nameShortCircuit() {
    left.setName("left");
    List<Diff<Object, Object>> diffs = new FieldSchemaComparator(SHORT_CIRCUIT).compare(left, right);
    assertThat(diffs, is(notNullValue()));
    assertThat(diffs.size(), is(1));
    assertThat(diffs.get(0), is(newPropertyDiff(FieldSchema.class, "name", "left", "name")));
  }

  @Test
  public void nameFullComparison() {
    left.setName("left");
    List<Diff<Object, Object>> diffs = new FieldSchemaComparator(FULL_COMPARISON).compare(left, right);
    assertThat(diffs, is(notNullValue()));
    assertThat(diffs.size(), is(1));
    assertThat(diffs.get(0), is(newPropertyDiff(FieldSchema.class, "name", "left", "name")));
  }

  @Test
  public void typeShortCircuit() {
    left.setType("int");
    List<Diff<Object, Object>> diffs = new FieldSchemaComparator(SHORT_CIRCUIT).compare(left, right);
    assertThat(diffs, is(notNullValue()));
    assertThat(diffs.size(), is(1));
    assertThat(diffs.get(0), is(newPropertyDiff(FieldSchema.class, "type", "int", "string")));
  }

  @Test
  public void typeFullComparison() {
    left.setType("int");
    List<Diff<Object, Object>> diffs = new FieldSchemaComparator(FULL_COMPARISON).compare(left, right);
    assertThat(diffs, is(notNullValue()));
    assertThat(diffs.size(), is(1));
    assertThat(diffs.get(0), is(newPropertyDiff(FieldSchema.class, "type", "int", "string")));
  }

  @Test
  public void commentShortCircuit() {
    left.setComment("left");
    List<Diff<Object, Object>> diffs = new FieldSchemaComparator(SHORT_CIRCUIT).compare(left, right);
    assertThat(diffs, is(notNullValue()));
    assertThat(diffs.size(), is(0));
  }

  @Test
  public void commentFullComparison() {
    left.setComment("left");
    List<Diff<Object, Object>> diffs = new FieldSchemaComparator(FULL_COMPARISON).compare(left, right);
    assertThat(diffs, is(notNullValue()));
    assertThat(diffs.size(), is(0));
  }

  @Test
  public void allShortCircuit() {
    left.setName("left");
    left.setType("int");
    left.setComment("left comment");
    List<Diff<Object, Object>> diffs = new FieldSchemaComparator(SHORT_CIRCUIT).compare(left, right);
    assertThat(diffs, is(notNullValue()));
    assertThat(diffs.size(), is(1));
    assertThat(diffs.get(0), is(newPropertyDiff(FieldSchema.class, "name", "left", "name")));
  }

  @Test
  public void allFullComparison() {
    left.setName("left");
    left.setType("int");
    left.setComment("left comment");
    List<Diff<Object, Object>> diffs = new FieldSchemaComparator(FULL_COMPARISON).compare(left, right);
    assertThat(diffs, is(notNullValue()));
    assertThat(diffs.size(), is(2));
    assertThat(diffs.get(0), is(newPropertyDiff(FieldSchema.class, "name", "left", "name")));
    assertThat(diffs.get(1), is(newPropertyDiff(FieldSchema.class, "type", "int", "string")));
  }

}
