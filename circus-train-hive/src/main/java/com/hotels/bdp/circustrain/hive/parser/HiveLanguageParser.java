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
package com.hotels.bdp.circustrain.hive.parser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.lib.DefaultGraphWalker;
import org.apache.hadoop.hive.ql.lib.DefaultRuleDispatcher;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.GraphWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.Rule;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.parse.SemanticException;

public class HiveLanguageParser {
  // These 2 are copies of the declarations in org.apache.hadoop.hive.ql.session.SessionState
  private final static String HDFS_SESSION_PATH_KEY = "_hive.hdfs.session.path";
  private final static String LOCAL_SESSION_PATH_KEY = "_hive.local.session.path";

  private static String hdfsTemporaryDirectory(HiveConf hiveConf) {
    return hiveConf.get("hadoop.tmp.dir", "/tmp");
  }

  private static String localTemporaryDirectory() {
    return System.getProperty("java.io.tmpdir", "/tmp");
  }

  private final HiveConf hiveConf;

  public HiveLanguageParser(HiveConf hiveConfiguration) {
    hiveConf = new HiveConf(hiveConfiguration);
    if (hiveConf.get(HDFS_SESSION_PATH_KEY) == null) {
      hiveConf.set(HDFS_SESSION_PATH_KEY, hdfsTemporaryDirectory(hiveConf));
    }
    if (hiveConf.get(LOCAL_SESSION_PATH_KEY) == null) {
      hiveConf.set(LOCAL_SESSION_PATH_KEY, localTemporaryDirectory());
    }
  }

  public void parse(String statement, NodeProcessor nodeProcessor) {
    Context parserContext;
    try {
      parserContext = new Context(hiveConf);
    } catch (IOException e) {
      throw new RuntimeException("Unable to create Context for parser", e);
    }
    ParseDriver parserDriver = new ParseDriver();

    ASTNode tree;
    try {
      tree = parserDriver.parse(statement, parserContext);
    } catch (ParseException e) {
      throw new HiveParseException(e);
    }
    // create a walker which walks the tree in a DFS manner while maintaining
    // the operator stack. The dispatcher
    // generates the plan from the operator tree
    Map<Rule, NodeProcessor> rules = new LinkedHashMap<>();
    // The dispatcher fires the processor corresponding to the closest matching
    // rule and passes the context along
    Dispatcher dispatcher = new DefaultRuleDispatcher(nodeProcessor, rules, null);
    GraphWalker walker = new DefaultGraphWalker(dispatcher);
    // Create a list of topop nodes
    List<Node> topNodes = new ArrayList<>();
    topNodes.add(tree);
    try {
      walker.startWalking(topNodes, null);
    } catch (SemanticException e) {
      throw new HiveSemanticException(e);
    }
  }
}
