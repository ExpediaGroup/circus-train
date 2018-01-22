/**
 * Copyright (C) 2016-2018 Expedia Inc.
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
package com.hotels.bdp.circustrain.hive.parser;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyVararg;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.Stack;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class HiveLanguageParserTest {

  private static final String CREATE_TABLE_STATEMENT = new StringBuilder()
      .append("CREATE EXTERNAL TABLE `db.tbl`(\n")
      .append("    `a` string, \n")
      .append("    `b` string, \n")
      .append("    `c` boolean, \n")
      .append("    `d` string)\n")
      .append("  COMMENT 'Generated by abc'\n")
      .append("  PARTITIONED BY ( \n")
      .append("    `part1` string, \n")
      .append("    `part2` int)\n")
      .append("  ROW FORMAT SERDE \n")
      .append("    'org.apache.hadoop.hive.ql.io.orc.OrcSerde'\n")
      .append("  STORED AS INPUTFORMAT \n")
      .append("    'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'\n")
      .append("  OUTPUTFORMAT \n")
      .append("    'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'\n")
      .append("  LOCATION\n")
      .append("    's3://bucket/location'\n")
      .append("  TBLPROPERTIES (\n")
      .append("    'DO_NOT_UPDATE_STATS'='true',\n")
      .append("    'STATS_GENERATED_VIA_STATS_TASK'='true',\n")
      .append("    'transient_lastDdlTime'='1484563371')\n")
      .toString();

  private @Mock NodeProcessor nodeProcessor;

  private final HiveLanguageParser parser = new HiveLanguageParser(new HiveConf());

  @SuppressWarnings("unchecked")
  @Test
  public void typical() throws Exception {
    parser.parse(CREATE_TABLE_STATEMENT, nodeProcessor);
    verify(nodeProcessor, times(49)).process(any(Node.class), any(Stack.class), any(NodeProcessorCtx.class),
        anyVararg());
  }

  @Test(expected = HiveParseException.class)
  public void invalidSyntax() throws Exception {
    parser.parse("CREATE TABLE abc () LOCATION 'path'", nodeProcessor);
  }

}
