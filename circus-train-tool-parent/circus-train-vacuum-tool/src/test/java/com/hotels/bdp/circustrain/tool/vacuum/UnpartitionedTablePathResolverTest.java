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
package com.hotels.bdp.circustrain.tool.vacuum;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

import java.util.Set;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class UnpartitionedTablePathResolverTest {

  private static final String TABLE_NAME = "table";
  private static final String DATABASE_NAME = "database";
  @Mock
  private Table table;
  @Mock
  private StorageDescriptor tableSd;

  @Test
  public void typical() throws Exception {
    when(table.getSd()).thenReturn(tableSd);
    when(table.getDbName()).thenReturn(DATABASE_NAME);
    when(table.getTableName()).thenReturn(TABLE_NAME);
    when(tableSd.getLocation()).thenReturn("file:///tmp/data/eventId");

    TablePathResolver resolver = new UnpartitionedTablePathResolver(table);

    Path globPath = resolver.getGlobPath();
    assertThat(globPath, is(new Path("file:///tmp/data/*")));

    Path tableBaseLocation = resolver.getTableBaseLocation();
    assertThat(tableBaseLocation, is(new Path("file:///tmp/data")));

    Set<Path> metastorePaths = resolver.getMetastorePaths((short) 100, 1000);
    assertThat(metastorePaths.size(), is(1));
    assertThat(metastorePaths.iterator().next(), is(new Path("file:///tmp/data/eventId")));
  }

}
