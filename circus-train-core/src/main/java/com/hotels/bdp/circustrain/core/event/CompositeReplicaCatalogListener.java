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
import com.hotels.bdp.circustrain.api.event.ReplicaCatalogListener;

public class CompositeReplicaCatalogListener implements ReplicaCatalogListener {

  private static final Logger LOG = LoggerFactory.getLogger(CompositeReplicaCatalogListener.class);

  private final List<ReplicaCatalogListener> listeners;

  public CompositeReplicaCatalogListener(List<ReplicaCatalogListener> listeners) {
    this.listeners = Collections.unmodifiableList(listeners);
  }

  @Override
  public void resolvedReplicaLocation(URI location) {
    for (final ReplicaCatalogListener listener : listeners) {
      try {
        listener.resolvedReplicaLocation(location);
      } catch (Exception e) {
        LOG.error("Listener '{}' threw exception on resolvedReplicaLocation.", listener, e);
      }
    }
  }

  @Override
  public void existingReplicaPartitions(EventPartitions partitions) {
    for (final ReplicaCatalogListener listener : listeners) {
      try {
        listener.existingReplicaPartitions(partitions);
      } catch (Exception e) {
        LOG.error("Listener '{}' threw exception on existingReplicaPartitions.", listener, e);
      }
    }
  }

  @Override
  public void partitionsToCreate(EventPartitions partitions) {
    for (final ReplicaCatalogListener listener : listeners) {
      try {
        listener.partitionsToCreate(partitions);
      } catch (Exception e) {
        LOG.error("Listener '{}' threw exception on partitionsToCreate.", listener, e);
      }
    }
  }

  @Override
  public void partitionsToAlter(EventPartitions partitions) {
    for (final ReplicaCatalogListener listener : listeners) {
      try {
        listener.partitionsToAlter(partitions);
      } catch (Exception e) {
        LOG.error("Listener '{}' threw exception on partitionsToAlter.", listener, e);
      }
    }
  }

  @Override
  public void deprecatedReplicaLocations(List<URI> locations) {
    for (final ReplicaCatalogListener listener : listeners) {
      try {
        listener.deprecatedReplicaLocations(locations);
      } catch (Exception e) {
        LOG.error("Listener '{}' threw exception on deprecatedReplicaLocations.", listener, e);
      }
    }
  }
}
