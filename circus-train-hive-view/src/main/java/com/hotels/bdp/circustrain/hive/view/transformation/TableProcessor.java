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

import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.SemanticException;

import com.google.common.collect.ImmutableList;

public class TableProcessor implements NodeProcessor {

  private final List<String> tables = new ArrayList<>();

  @Override
  public Object process(Node node, Stack<Node> stack, NodeProcessorCtx procCtx, Object... nodeOutputs)
    throws SemanticException {
    ASTNode astNode = (ASTNode) node;
    if (astNode.getToken() != null && astNode.getToken().getText() != null) {
      switch (astNode.getToken().getText()) {
      case "TOK_TABNAME":
        tables.add(extractTableName(astNode));
        break;
      }
    }
    return null;
  }

  private String extractTableName(ASTNode tabNameNode) {
    try {
      return BaseSemanticAnalyzer.getDotName(BaseSemanticAnalyzer.getQualifiedTableName(tabNameNode));
    } catch (SemanticException e) {
      throw new RuntimeException("Unable to extract qualified table name from node: " + tabNameNode.dump(), e);
    }
  }

  public List<String> getTables() {
    return ImmutableList.copyOf(tables);
  }

}
