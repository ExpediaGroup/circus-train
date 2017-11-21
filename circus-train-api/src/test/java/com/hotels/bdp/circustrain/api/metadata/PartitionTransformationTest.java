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
package com.hotels.bdp.circustrain.api.metadata;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet;
import org.apache.hadoop.hive.metastore.api.PrivilegeGrantInfo;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.SkewedInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

public class PartitionTransformationTest {

  private Partition partition;

  @Before
  public void init() {
    partition = new Partition();
    partition.setDbName("database");
    partition.setTableName("table");
    partition.setValues(ImmutableList.of("part"));

    Map<String, List<PrivilegeGrantInfo>> userPrivileges = new HashMap<>();
    userPrivileges.put("read", ImmutableList.of(new PrivilegeGrantInfo()));
    PrincipalPrivilegeSet privileges = new PrincipalPrivilegeSet();
    privileges.setUserPrivileges(userPrivileges);
    partition.setPrivileges(privileges);

    StorageDescriptor storageDescriptor = new StorageDescriptor();
    storageDescriptor.setCols(Arrays.asList(new FieldSchema("a", "int", null)));
    storageDescriptor.setInputFormat("input_format");
    storageDescriptor.setOutputFormat("output_format");
    storageDescriptor.setSerdeInfo(new SerDeInfo("serde", "lib", new HashMap<String, String>()));
    storageDescriptor.setSkewedInfo(new SkewedInfo());
    storageDescriptor.setParameters(new HashMap<String, String>());
    storageDescriptor.setLocation("database/table/part/");
    partition.setSd(storageDescriptor);

    Map<String, String> parameters = new HashMap<>();
    parameters.put("com.company.parameter", "abc");
    partition.setParameters(parameters);
  }

  @Test
  public void identity() {
    Partition partitionCopy = partition.deepCopy();
    Partition transformedPartition = PartitionTransformation.IDENTITY.transform(partition);
    assertThat(partition, is(partitionCopy)); // original partition is untouched
    assertThat(transformedPartition, is(partitionCopy)); // returned partition is verbatim copy of partition
    assertThat(transformedPartition == partition, is(true));
  }

}
