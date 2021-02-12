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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mockStatic;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

import org.antlr.runtime.Token;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ SessionState.class })
public class TableProcessorTest {

  private final static String DEFAULT_DATABASE = "bdp";
  private final static String DATABASE = "db";
  private final static String TABLE = "t1";

  private @Mock Token token;
  private @Mock ASTNode node;
  private @Mock ASTNode dbNode;
  private @Mock ASTNode tableNode;
  private @Mock SessionState sessionState;

  private final TableProcessor processor = new TableProcessor();

  @Before
  public void init() throws Exception {
    mockStatic(SessionState.class);
    when(dbNode.getText()).thenReturn(DATABASE);
    when(tableNode.getText()).thenReturn(TABLE);
    when(node.getText()).thenReturn(DATABASE + "." + TABLE);
    when(node.getToken()).thenReturn(token);
    when(sessionState.getCurrentDatabase()).thenReturn(DEFAULT_DATABASE);
  }

  @Test
  public void typical() throws Exception {
    ArrayList<Node> children = new ArrayList<>();
    children.add(dbNode);
    children.add(tableNode);
    when(node.getChildren()).thenReturn(children);
    when(node.getChildCount()).thenReturn(children.size());
    when(node.getChild(0)).thenReturn(dbNode);
    when(node.getChild(1)).thenReturn(tableNode);
    when(node.getType()).thenReturn(HiveParser.TOK_TABNAME);
    when(token.getText()).thenReturn("TOK_TABNAME");
    processor.process(node, null, null);
    assertThat(processor.getTables(), is(Arrays.asList(DATABASE + "." + TABLE)));
  }

  @Test
  public void unqualifiedTableName() throws Exception {
    ArrayList<Node> children = new ArrayList<>();
    children.add(tableNode);
    when(node.getChildren()).thenReturn(children);
    when(node.getChildCount()).thenReturn(children.size());
    when(node.getChild(0)).thenReturn(tableNode);
    when(node.getType()).thenReturn(HiveParser.TOK_TABNAME);
    when(token.getText()).thenReturn("TOK_TABNAME");
    when(SessionState.get()).thenReturn(sessionState);
    processor.process(node, null, null);
    assertThat(processor.getTables(), is(Arrays.asList(DEFAULT_DATABASE + "." + TABLE)));
  }

  @Test(expected = NullPointerException.class)
  public void unqualifiedTableNameFailsIsSessionIsNotSet() throws Exception {
    ArrayList<Node> children = new ArrayList<>();
    children.add(tableNode);
    when(node.getChildren()).thenReturn(children);
    when(node.getChildCount()).thenReturn(children.size());
    when(node.getChild(0)).thenReturn(tableNode);
    when(node.getType()).thenReturn(HiveParser.TOK_TABNAME);
    when(token.getText()).thenReturn("TOK_TABNAME");
    processor.process(node, null, null);
  }

  @Test
  public void noMatch() throws Exception {
    when(token.getText()).thenReturn("TOK_X");
    processor.process(node, null, null);
    assertThat(processor.getTables(), is(Collections.<String> emptyList()));
  }

}
