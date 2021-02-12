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
package com.hotels.bdp.circustrain.core;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.hotels.bdp.circustrain.api.conf.SourceTable;
import com.hotels.bdp.circustrain.api.conf.TableReplication;
import com.hotels.bdp.circustrain.core.conf.SpringExpressionParser;

@RunWith(MockitoJUnitRunner.class)
public class SpelParsedPartitionPredicateTest {

  private @Mock SpringExpressionParser expressionParser;
  private @Mock TableReplication tableReplication;
  private @Mock SourceTable sourceTable;

  private SpelParsedPartitionPredicate predicate;

  @Before
  public void setUp() {
    when(tableReplication.getSourceTable()).thenReturn(sourceTable);
    predicate = new SpelParsedPartitionPredicate(expressionParser, tableReplication);
  }

  @Test
  public void simpleFilter() throws Exception {
    when(sourceTable.getPartitionFilter()).thenReturn("filter");
    when(expressionParser.parse("filter")).thenReturn("filter");

    assertThat(predicate.getPartitionPredicate(), is("filter"));
  }

  @Test
  public void expressionParserChangedFilter() throws Exception {
    when(expressionParser.parse("filter")).thenReturn("filter2");
    when(sourceTable.getPartitionFilter()).thenReturn("filter");

    assertThat(predicate.getPartitionPredicate(), is("filter2"));
  }

  @Test
  public void partitionPredicateLimit() throws Exception {
    when(sourceTable.getPartitionLimit()).thenReturn((short) 10);
    assertThat(predicate.getPartitionPredicateLimit(), is((short) 10));
  }

  @Test
  public void noPartitionPredicateLimitSetDefaultsToMinus1() throws Exception {
    when(sourceTable.getPartitionLimit()).thenReturn(null);
    assertThat(predicate.getPartitionPredicateLimit(), is((short) -1));
  }
}
