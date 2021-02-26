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

import java.util.List;

import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.Test;

import com.hotels.bdp.circustrain.hive.parser.HiveLanguageParser;

public class TableProcessorIntegrationTest {

  private static final String SELECT_STATEMENT = new StringBuilder()
      .append("SELECT a.col1, b.col2 \n")
      .append("  FROM bdp.t1 AS a \n")
      .append("  JOIN hcom.t2 AS b ON b.key = a.key \n")
      .append(" WHERE a.col3 = 'awesome' \n")
      .toString();

  private final TableProcessor processor = new TableProcessor();

  @Test
  public void typical() {
    HiveLanguageParser parser = new HiveLanguageParser(new HiveConf());
    parser.parse(SELECT_STATEMENT, processor);
    List<String> tables = processor.getTables();

    assertThat(tables.size(), is(2));
    assertThat(tables.get(0), is("bdp.t1"));
    assertThat(tables.get(1), is("hcom.t2"));
  }
}
