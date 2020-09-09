/**
 * Copyright (C) 2016-2020 Expedia, Inc.
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
package com.hotels.bdp.circustrain.api.copier;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.hotels.bdp.circustrain.api.conf.TableReplication;

@RunWith(MockitoJUnitRunner.class)
public class CopierContextTest {

  private @Mock TableReplication tableReplication;
  private @Mock Map<String, Object> copierOptions;
  private @Mock Table sourceTable;
  private @Mock List<Partition> sourcePartitions;

  private final String eventId = "1";
  private final Path sourceLocation = new Path("source");
  private final List<Path> sourceSubLocations = new ArrayList<>();
  private final Path replicaLocation = new Path("replica");

  private CopierContext copierContext;

  @Before
  public void init() {
    copierContext = new CopierContext(tableReplication, eventId, sourceLocation, sourceSubLocations,
        replicaLocation, copierOptions, sourceTable, sourcePartitions);
  }

  @Test
  public void getters() {
    assertThat(copierContext.getEventId(), is("1"));
    assertThat(copierContext.getSourceBaseLocation(), is(new Path("source")));
    assertThat(copierContext.getSourceSubLocations().size(), is(0));
    assertThat(copierContext.getReplicaLocation(), is(new Path("replica")));

    assertThat(copierContext.getTableReplication(), is(tableReplication));
    assertThat(copierContext.getCopierOptions(), is(copierOptions));
    assertThat(copierContext.getSourceTable(), is(sourceTable));
    assertThat(copierContext.getSourcePartitions(), is(sourcePartitions));
  }
}
