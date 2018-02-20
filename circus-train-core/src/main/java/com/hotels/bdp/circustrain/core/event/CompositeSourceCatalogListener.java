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
package com.hotels.bdp.circustrain.core.event;

import java.net.URI;
import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hotels.bdp.circustrain.api.event.EventPartitions;
import com.hotels.bdp.circustrain.api.event.EventTable;
import com.hotels.bdp.circustrain.api.event.SourceCatalogListener;

public class CompositeSourceCatalogListener implements SourceCatalogListener {
  private static final Logger LOG = LoggerFactory.getLogger(CompositeSourceCatalogListener.class);

  private final List<SourceCatalogListener> listeners;

  public CompositeSourceCatalogListener(List<SourceCatalogListener> listeners) {
    this.listeners = Collections.unmodifiableList(listeners);
  }

  @Override
  public void resolvedMetaStoreSourceTable(EventTable table) {
    for (final SourceCatalogListener listener : listeners) {
      try {
        listener.resolvedMetaStoreSourceTable(table);
      } catch (Exception e) {
        LOG.error("Listener '{}' threw exception on resolvedMetaStoreSourceTable.", listener, e);
      }
    }
  }

  @Override
  public void resolvedSourcePartitions(EventPartitions partitions) {
    for (final SourceCatalogListener listener : listeners) {
      try {
        listener.resolvedSourcePartitions(partitions);
      } catch (Exception e) {
        LOG.error("Listener '{}' threw exception on resolvedSourcePartitions.", listener, e);
      }
    }
  }

  @Override
  public void resolvedSourceLocation(URI location) {
    for (final SourceCatalogListener listener : listeners) {
      try {
        listener.resolvedSourceLocation(location);
      } catch (Exception e) {
        LOG.error("Listener '{}' threw exception on resolvedSourceLocation.", listener, e);
      }
    }
  }

}
