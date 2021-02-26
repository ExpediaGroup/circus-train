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
package com.hotels.bdp.circustrain.hive.view.transformation;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import org.junit.Test;


public class TableTranslationTest {

  private TableTranslation tableTranslation = new TableTranslation("odb", "o_table", "rdb", "r_table");

  @Test
  public void unescapedOriginalName() {
    assertThat(tableTranslation.toUnescapedQualifiedOriginalName(), is("odb.o_table"));
  }

  @Test
  public void escapedOriginalName() {
    assertThat(tableTranslation.toEscapedQualifiedOriginalName(), is("`odb`.`o_table`"));
  }

  @Test
  public void unescapedReplicaName() {
    assertThat(tableTranslation.toUnescapedQualifiedReplicaName(), is("rdb.r_table"));
  }

  @Test
  public void escapedReplicaName() {
    assertThat(tableTranslation.toEscapedQualifiedReplicaName(), is("`rdb`.`r_table`"));
  }

  @Test
  public void unescapedOriginalTableReference() {
    assertThat(tableTranslation.toUnescapedOriginalTableReference(), is("o_table."));
  }

  @Test
  public void escapedOriginalTableReference() {
    assertThat(tableTranslation.toEscapedOriginalTableReference(), is("`o_table`."));
  }

  @Test
  public void unescapedReplicaTableReference() {
    assertThat(tableTranslation.toUnescapedReplicaTableReference(), is("r_table."));
  }

  @Test
  public void escapedReplicaTableReference() {
    assertThat(tableTranslation.toEscapedReplicaTableReference(), is("`r_table`."));
  }

}
