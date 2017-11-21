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
package com.hotels.bdp.circustrain.core;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.base.Function;

import com.hotels.bdp.circustrain.core.conf.SourceTable;
import com.hotels.bdp.circustrain.core.conf.SpringExpressionParser;
import com.hotels.bdp.circustrain.core.conf.TableReplication;
import com.hotels.bdp.circustrain.core.replica.Replica;
import com.hotels.bdp.circustrain.core.replica.ReplicaFactory;
import com.hotels.bdp.circustrain.core.source.Source;
import com.hotels.bdp.circustrain.core.source.SourceFactory;

@RunWith(MockitoJUnitRunner.class)
public class PartitionPredicateFactoryTest {

  @Mock
  private SourceFactory sourceFactory;
  @Mock
  private ReplicaFactory replicaFactory;
  @Mock
  private SpringExpressionParser expressionParser;
  @Mock
  private Function<Path, String> checksumFunction;
  @Mock
  private TableReplication tableReplication;
  @Mock
  private SourceTable sourceTable;
  @Mock
  private Source source;
  @Mock
  private Replica replica;

  private PartitionPredicateFactory partitionPredicateFactory;

  @Before
  public void setUp() {
    partitionPredicateFactory = new PartitionPredicateFactory(sourceFactory, replicaFactory, expressionParser,
        checksumFunction);
    when(tableReplication.getSourceTable()).thenReturn(sourceTable);
  }

  @Test
  public void newInstanceSpelParsedPartitionPredicate() throws Exception {
    when(sourceTable.isGeneratePartitionFilter()).thenReturn(false);
    PartitionPredicate predicate = partitionPredicateFactory.newInstance(tableReplication);
    assertThat(predicate, instanceOf(SpelParsedPartitionPredicate.class));
  }

  @Test
  public void newInstanceDiffGeneratedPartitionPredicate() throws Exception {
    when(sourceTable.isGeneratePartitionFilter()).thenReturn(true);
    when(sourceFactory.newInstance(tableReplication)).thenReturn(source);
    when(replicaFactory.newInstance(tableReplication)).thenReturn(replica);
    PartitionPredicate predicate = partitionPredicateFactory.newInstance(tableReplication);
    assertThat(predicate, instanceOf(DiffGeneratedPartitionPredicate.class));
  }
}
