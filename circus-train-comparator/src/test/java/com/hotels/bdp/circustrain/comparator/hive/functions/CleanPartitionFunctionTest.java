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
package com.hotels.bdp.circustrain.comparator.hive.functions;

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;

import static com.hotels.bdp.circustrain.comparator.TestUtils.COLS;
import static com.hotels.bdp.circustrain.comparator.TestUtils.CREATE_TIME;
import static com.hotels.bdp.circustrain.comparator.TestUtils.DATABASE;
import static com.hotels.bdp.circustrain.comparator.TestUtils.INPUT_FORMAT;
import static com.hotels.bdp.circustrain.comparator.TestUtils.OUTPUT_FORMAT;
import static com.hotels.bdp.circustrain.comparator.TestUtils.TABLE;
import static com.hotels.bdp.circustrain.comparator.TestUtils.newPartitionAndMetadata;

import java.util.List;

import org.junit.Test;

import com.google.common.collect.ImmutableList;

import com.hotels.bdp.circustrain.api.CircusTrainTableParameter;
import com.hotels.bdp.circustrain.comparator.api.ExcludedHiveParameter;
import com.hotels.bdp.circustrain.comparator.hive.wrappers.PartitionAndMetadata;

public class CleanPartitionFunctionTest {

  private final CleanPartitionFunction function = new CleanPartitionFunction();

  @Test
  public void verbatimCopy() {
    PartitionAndMetadata partition = newPartitionAndMetadata(DATABASE, TABLE, "val");
    PartitionAndMetadata partitionCopy = function.apply(partition);
    assertFalse(partitionCopy == partition);
    assertThat(partitionCopy, is(partition));
  }

  @Test
  public void cleansedCopy() {
    PartitionAndMetadata partition = newPartitionAndMetadata(DATABASE, TABLE, "val");
    for (CircusTrainTableParameter p : CircusTrainTableParameter.values()) {
      partition.getPartition().getParameters().put(p.parameterName(), "sourceTable");
    }
    for (ExcludedHiveParameter p : ExcludedHiveParameter.values()) {
      partition.getPartition().getParameters().put(p.parameterName(), "1234567890");
    }
    PartitionAndMetadata partitionCopy = function.apply(partition);
    assertFalse(partitionCopy == partition);
    assertThat(partitionCopy, is(not(partition)));

    assertThat(partitionCopy.getPartition().getDbName(), is(DATABASE));
    assertThat(partitionCopy.getPartition().getTableName(), is(TABLE));
    assertThat(partitionCopy.getPartition().getCreateTime(), is(CREATE_TIME));
    List<String> partitionValues = ImmutableList.of("val");
    assertThat(partitionCopy.getPartition().getValues(), is(partitionValues));

    assertThat(partitionCopy.getPartition().getPrivileges().getUserPrivileges().size(), is(1));
    assertThat(partitionCopy.getPartition().getPrivileges().getUserPrivileges().get("read"), is(notNullValue()));

    assertThat(partitionCopy.getPartition().getSd().getCols(), is(COLS));
    assertThat(partitionCopy.getPartition().getSd().getInputFormat(), is(INPUT_FORMAT));
    assertThat(partitionCopy.getPartition().getSd().getOutputFormat(), is(OUTPUT_FORMAT));

    assertThat(partitionCopy.getPartition().getParameters().size(), is(1));
    assertThat(partitionCopy.getPartition().getParameters().get("com.company.parameter"), is("abc"));
  }

}
