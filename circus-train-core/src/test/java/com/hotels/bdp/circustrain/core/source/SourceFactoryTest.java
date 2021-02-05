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
package com.hotels.bdp.circustrain.core.source;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.base.Supplier;

import com.hotels.bdp.circustrain.api.conf.ReplicationMode;
import com.hotels.bdp.circustrain.api.conf.SourceCatalog;
import com.hotels.bdp.circustrain.api.conf.SourceTable;
import com.hotels.bdp.circustrain.api.conf.TableReplication;
import com.hotels.bdp.circustrain.api.event.SourceCatalogListener;
import com.hotels.hcommon.hive.metastore.client.api.CloseableMetaStoreClient;

@RunWith(MockitoJUnitRunner.class)
public class SourceFactoryTest {

  private @Mock SourceCatalog sourceCatalog;
  private @Mock HiveConf sourceHiveConf;
  private @Mock Supplier<CloseableMetaStoreClient> sourceMetaStoreClientSupplier;
  private @Mock SourceCatalogListener sourceCatalogListener;
  private @Mock TableReplication tableReplication;
  private @Mock SourceTable sourceTable;

  @Before
  public void init() {
    when(tableReplication.getSourceTable()).thenReturn(sourceTable);
  }

  @Test
  public void newInstanceFullReplicationMode() throws Exception {
    when(tableReplication.getReplicationMode()).thenReturn(ReplicationMode.FULL);
    when(sourceCatalog.isDisableSnapshots()).thenReturn(false);
    SourceFactory sourceFactory = new SourceFactory(sourceCatalog, sourceHiveConf, sourceMetaStoreClientSupplier,
        sourceCatalogListener);
    Source source = sourceFactory.newInstance(tableReplication);
    assertFalse(source.isSnapshotsDisabled());
  }

  @Test
  public void newInstanceMetadataMirrorReplicationModeOverridesDisabledSnapshots() throws Exception {
    when(tableReplication.getReplicationMode()).thenReturn(ReplicationMode.METADATA_MIRROR);
    SourceFactory sourceFactory = new SourceFactory(sourceCatalog, sourceHiveConf, sourceMetaStoreClientSupplier,
        sourceCatalogListener);
    Source source = sourceFactory.newInstance(tableReplication);
    assertTrue(source.isSnapshotsDisabled());
    verify(sourceCatalog, never()).isDisableSnapshots();
  }

  @Test
  public void newInstanceMetadataUpdateReplicationModeOverridesDisabledSnapshots() throws Exception {
    when(tableReplication.getReplicationMode()).thenReturn(ReplicationMode.METADATA_UPDATE);
    SourceFactory sourceFactory = new SourceFactory(sourceCatalog, sourceHiveConf, sourceMetaStoreClientSupplier,
        sourceCatalogListener);
    Source source = sourceFactory.newInstance(tableReplication);
    assertTrue(source.isSnapshotsDisabled());
    verify(sourceCatalog, never()).isDisableSnapshots();
  }
}
