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
package com.hotels.bdp.circustrain.core.event;

import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hotels.bdp.circustrain.api.event.EventTableReplication;
import com.hotels.bdp.circustrain.api.event.TableReplicationListener;

public class CompositeTableReplicationListener implements TableReplicationListener {

  private static final Logger LOG = LoggerFactory.getLogger(CompositeTableReplicationListener.class);

  private final List<TableReplicationListener> listeners;

  public CompositeTableReplicationListener(List<TableReplicationListener> listeners) {
    this.listeners = Collections.unmodifiableList(listeners);
  }

  @Override
  public void tableReplicationStart(EventTableReplication tableReplication, String eventId) {
    for (final TableReplicationListener listener : listeners) {
      try {
        listener.tableReplicationStart(tableReplication, eventId);
      } catch (Exception e) {
        LOG.error("Listener '{}' threw exception on tableReplicationStart.", listener, e);
      }
    }
  }

  @Override
  public void tableReplicationSuccess(EventTableReplication tableReplication, String eventId) {
    for (final TableReplicationListener listener : listeners) {
      try {
        listener.tableReplicationSuccess(tableReplication, eventId);
      } catch (Exception e) {
        LOG.error("Listener '{}' threw exception on tableReplicationSuccess.", listener, e);
      }
    }
  }

  @Override
  public void tableReplicationFailure(EventTableReplication tableReplication, String eventId, Throwable t) {
    for (final TableReplicationListener listener : listeners) {
      try {
        listener.tableReplicationFailure(tableReplication, eventId, t);
      } catch (Exception e) {
        LOG.error("Listener '{}' threw exception on tableReplicationFailure.", listener, e);
      }
    }
  }
}
