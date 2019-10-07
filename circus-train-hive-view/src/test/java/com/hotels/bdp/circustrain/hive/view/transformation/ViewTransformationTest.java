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
package com.hotels.bdp.circustrain.hive.view.transformation;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.base.Supplier;

import com.hotels.bdp.circustrain.api.CircusTrainException;
import com.hotels.hcommon.hive.metastore.client.api.CloseableMetaStoreClient;

@RunWith(MockitoJUnitRunner.class)
public class ViewTransformationTest {

  private static final String DATABASE = "database";
  private static final String TABLE = "table";
  private static final String QUALIFIED_NAME = String.format("%s.%s", DATABASE, TABLE);
  private static final String ORIGINAL_HQL = "SELECT * FROM t";
  private static final String EXPANDED_HQL = "SELECT `a` FROM `original`.`t`";
  private static final String ORIGINAL_TRANSLATED_HQL = "SELECT * FROM tt";
  private static final String EXPANDED_TRANSLATED_HQL = "SELECT `a` FROM `replica`.`tt`";

  private @Mock Table table;
  private @Mock HqlTranslator translator;
  private @Mock Supplier<CloseableMetaStoreClient> replicaMetaStoreClientSupplier;
  private @Mock CloseableMetaStoreClient metaStoreClient;

  private HiveConf hiveConf = new HiveConf();
  private ViewTransformation transformation;

  @Before
  public void init() {
    hiveConf.setBoolean(ViewTransformation.SKIP_TABLE_EXIST_CHECKS, true);

    when(replicaMetaStoreClientSupplier.get()).thenReturn(metaStoreClient);

    when(table.getDbName()).thenReturn(DATABASE);
    when(table.getTableName()).thenReturn(TABLE);
    when(table.getTableType()).thenReturn(TableType.VIRTUAL_VIEW.name());
    when(table.getViewOriginalText()).thenReturn(ORIGINAL_HQL);
    when(table.getViewExpandedText()).thenReturn(EXPANDED_HQL);
    when(translator.translate(QUALIFIED_NAME, ORIGINAL_HQL)).thenReturn(ORIGINAL_TRANSLATED_HQL);
    when(translator.translate(QUALIFIED_NAME, EXPANDED_HQL)).thenReturn(EXPANDED_TRANSLATED_HQL);
    transformation = new ViewTransformation(hiveConf, translator, replicaMetaStoreClientSupplier);
  }

  @Test
  public void skipTables() {
    when(table.getTableType()).thenReturn(TableType.EXTERNAL_TABLE.name());
    Table transformedTable = transformation.transform(table);
    assertThat(transformedTable, is(table));
  }

  @Test
  public void transform() {
    Table transformedTable = transformation.transform(table);
    assertThat(transformedTable, is(not(table)));
    verify(translator).translate(QUALIFIED_NAME, ORIGINAL_HQL);
    verify(translator).translate(QUALIFIED_NAME, EXPANDED_HQL);
    assertThat(transformedTable.getViewOriginalText(), is(ORIGINAL_TRANSLATED_HQL));
    assertThat(transformedTable.getViewExpandedText(), is(EXPANDED_TRANSLATED_HQL));
  }

  @Test(expected = CircusTrainException.class)
  public void underlyingTableIsMissing() throws Exception {
    hiveConf.setBoolean(ViewTransformation.SKIP_TABLE_EXIST_CHECKS, false);
    when(metaStoreClient.getTable("replica", "tt")).thenThrow(new NoSuchObjectException());
    transformation.transform(table);
  }

  @Test
  public void underlyingTablesExist() {
    hiveConf.setBoolean(ViewTransformation.SKIP_TABLE_EXIST_CHECKS, false);
    when(table.getTableType()).thenReturn(TableType.VIRTUAL_VIEW.name());
    Table transformedTable = transformation.transform(table);
    assertThat(transformedTable, is(not(table)));
    verify(translator).translate(QUALIFIED_NAME, ORIGINAL_HQL);
    verify(translator).translate(QUALIFIED_NAME, EXPANDED_HQL);
    assertThat(transformedTable.getViewOriginalText(), is(ORIGINAL_TRANSLATED_HQL));
    assertThat(transformedTable.getViewExpandedText(), is(EXPANDED_TRANSLATED_HQL));
  }

}
