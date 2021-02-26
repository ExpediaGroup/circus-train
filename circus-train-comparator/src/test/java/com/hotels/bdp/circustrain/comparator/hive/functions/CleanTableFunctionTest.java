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
package com.hotels.bdp.circustrain.comparator.hive.functions;

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertFalse;

import static com.hotels.bdp.circustrain.comparator.TestUtils.COLS;
import static com.hotels.bdp.circustrain.comparator.TestUtils.CREATE_TIME;
import static com.hotels.bdp.circustrain.comparator.TestUtils.DATABASE;
import static com.hotels.bdp.circustrain.comparator.TestUtils.INPUT_FORMAT;
import static com.hotels.bdp.circustrain.comparator.TestUtils.OUTPUT_FORMAT;
import static com.hotels.bdp.circustrain.comparator.TestUtils.OWNER;
import static com.hotels.bdp.circustrain.comparator.TestUtils.TABLE;
import static com.hotels.bdp.circustrain.comparator.TestUtils.newTableAndMetadata;

import org.junit.Test;

import com.hotels.bdp.circustrain.api.CircusTrainTableParameter;
import com.hotels.bdp.circustrain.comparator.api.ExcludedHiveParameter;
import com.hotels.bdp.circustrain.comparator.hive.wrappers.TableAndMetadata;

public class CleanTableFunctionTest {

  private final CleanTableFunction function = new CleanTableFunction();

  @Test
  public void verbatimCopy() {
    TableAndMetadata table = newTableAndMetadata(DATABASE, TABLE);
    TableAndMetadata tableCopy = function.apply(table);
    assertFalse(tableCopy == table);
    assertThat(tableCopy, is(table));
  }

  @Test
  public void cleansedCopy() {
    TableAndMetadata table = newTableAndMetadata(DATABASE, TABLE);
    for (CircusTrainTableParameter p : CircusTrainTableParameter.values()) {
      table.getTable().getParameters().put(p.parameterName(), "sourceTable");
    }
    for (ExcludedHiveParameter p : ExcludedHiveParameter.values()) {
      table.getTable().getParameters().put(p.parameterName(), "1234567890");
    }
    TableAndMetadata tableCopy = function.apply(table);
    assertFalse(tableCopy == table);
    assertThat(tableCopy, is(not(table)));

    assertThat(tableCopy.getTable().getDbName(), is(DATABASE));
    assertThat(tableCopy.getTable().getTableName(), is(TABLE));
    assertThat(tableCopy.getTable().getCreateTime(), is(CREATE_TIME));
    assertThat(tableCopy.getTable().getOwner(), is(OWNER));

    assertThat(tableCopy.getTable().getPrivileges().getUserPrivileges().size(), is(1));
    assertThat(tableCopy.getTable().getPrivileges().getUserPrivileges().get("read"), is(notNullValue()));

    assertThat(tableCopy.getTable().getSd().getCols(), is(COLS));
    assertThat(tableCopy.getTable().getSd().getInputFormat(), is(INPUT_FORMAT));
    assertThat(tableCopy.getTable().getSd().getOutputFormat(), is(OUTPUT_FORMAT));

    assertThat(tableCopy.getTable().getParameters().size(), is(1));
    assertThat(tableCopy.getTable().getParameters().get("com.company.parameter"), is("abc"));
  }

}
