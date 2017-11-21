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
package com.hotels.bdp.circustrain.hive.view.transformation;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import com.hotels.bdp.circustrain.core.conf.ReplicaTable;
import com.hotels.bdp.circustrain.core.conf.SourceTable;
import com.hotels.bdp.circustrain.core.conf.TableReplication;
import com.hotels.bdp.circustrain.core.conf.TableReplications;

@RunWith(MockitoJUnitRunner.class)
public class HqlTranslatorTest {

  private static final String VIEW_NAME = "db.view";
  private static final String UNESCAPED_SELECT_STATEMENT = new StringBuilder()
      .append("SELECT TABLE_A.col1, b.col2 \n")
      .append("  FROM db1.table_a \n")
      .append("  JOIN db2.table_b AS B ON B.key = table_a.key \n")
      .append(" WHERE table_a.cond = 'VAL' \n")
      .append("   AND TABLE_A.cmp < b.cmp \n")
      .toString();
  private static final String ESCAPED_SELECT_STATEMENT = new StringBuilder()
      .append("SELECT `A`.`col1`, `table_b`.`col2` \n")
      .append("  FROM `db1`.`table_a` AS `A` \n")
      .append("  JOIN `db2`.`table_b` ON `table_b`.`key` = `A`.`key` \n")
      .append(" WHERE `A`.`cond` = 'VAL' \n")
      .append("   AND `A`.`cmp` < `table_b`.`cmp` \n")
      .toString();

  private @Mock SourceTable sourceTable;
  private @Mock ReplicaTable replicaTable;
  private @Mock TableReplication tableReplication;
  private TableReplications tableReplications;

  @Before
  public void init() {
    when(sourceTable.getQualifiedName()).thenReturn(VIEW_NAME);
    when(tableReplication.getSourceTable()).thenReturn(sourceTable);
    when(tableReplication.getReplicaTable()).thenReturn(replicaTable);
    List<TableReplication> tableReplicationList = ImmutableList
        .<TableReplication> builder()
        .add(tableReplication)
        .build();
    tableReplications = new TableReplications();
    tableReplications.setTableReplications(tableReplicationList);
  }

  @Test
  public void mappings() {
    Map<String, String> replicationMappings = ImmutableMap
        .<String, String> builder()
        .put("db1.table_a", "r_db.a_table")
        .put("odb.o_table", "r_db.r_table")
        .build();
    when(tableReplication.getTableMappings()).thenReturn(replicationMappings);
    HqlTranslator translator = new HqlTranslator(tableReplications);
    assertThat(translator.getMappings().size(), is(1));
    List<TableTranslation> translations = translator.getMappings().get(VIEW_NAME);
    assertThat(translations.size(), is(2));
    assertThat(translations.contains(new TableTranslation("db1", "table_a", "r_db", "a_table")), is(true));
    assertThat(translations.contains(new TableTranslation("odb", "o_table", "r_db", "r_table")), is(true));
  }

  @Test
  public void unescapedStatement() {
    Map<String, String> replicationMappings = ImmutableMap
        .<String, String> builder()
        .put("db1.table_a", "r_db.a_table")
        .build();
    when(tableReplication.getTableMappings()).thenReturn(replicationMappings);
    HqlTranslator translator = new HqlTranslator(tableReplications);
    String translatedStatement = translator.translate(VIEW_NAME, UNESCAPED_SELECT_STATEMENT);
    String expectedTranslatedStatement = new StringBuilder()
        .append("SELECT a_table.col1, b.col2 \n")
        .append("  FROM r_db.a_table \n")
        .append("  JOIN db2.table_b AS B ON B.key = a_table.key \n")
        .append(" WHERE a_table.cond = 'VAL' \n")
        .append("   AND a_table.cmp < b.cmp \n")
        .toString();
    assertThat(translatedStatement, is(expectedTranslatedStatement));
  }

  @Test
  public void escapedStatment() {
    Map<String, String> replicationMappings = ImmutableMap
        .<String, String> builder()
        .put("db1.table_a", "r_db.a_table")
        .put("db2.table_b", "r_db.b_table")
        .build();
    when(tableReplication.getTableMappings()).thenReturn(replicationMappings);
    HqlTranslator translator = new HqlTranslator(tableReplications);
    String translatedStatement = translator.translate(VIEW_NAME, ESCAPED_SELECT_STATEMENT);
    String expectedTranslatedStatement = new StringBuilder()
        .append("SELECT `A`.`col1`, `b_table`.`col2` \n")
        .append("  FROM `r_db`.`a_table` AS `A` \n")
        .append("  JOIN `r_db`.`b_table` ON `b_table`.`key` = `A`.`key` \n")
        .append(" WHERE `A`.`cond` = 'VAL' \n")
        .append("   AND `A`.`cmp` < `b_table`.`cmp` \n")
        .toString();
    assertThat(translatedStatement, is(expectedTranslatedStatement));
  }

}
