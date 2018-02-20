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
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hotels.bdp.circustrain.api.CompletionCode;
import com.hotels.bdp.circustrain.api.event.EventReplicaCatalog;
import com.hotels.bdp.circustrain.api.event.EventSourceCatalog;
import com.hotels.bdp.circustrain.api.event.LocomotiveListener;

public class CompositeLocomotiveListener implements LocomotiveListener {

  private static final Logger LOG = LoggerFactory.getLogger(CompositeLocomotiveListener.class);

  private final List<LocomotiveListener> listeners;

  public CompositeLocomotiveListener(List<LocomotiveListener> listeners) {
    this.listeners = Collections.unmodifiableList(listeners);
  }

  @Override
  public void circusTrainStartUp(String[] args, EventSourceCatalog sourceCatalog, EventReplicaCatalog replicaCatalog) {
    for (final LocomotiveListener listener : listeners) {
      try {
        listener.circusTrainStartUp(args, sourceCatalog, replicaCatalog);
      } catch (Exception e) {
        LOG.error("Listener '{}' threw exception on circusTrainStartUp.", listener, e);
      }
    }
  }

  @Override
  public void circusTrainShutDown(CompletionCode completionCode, Map<String, Long> metrics) {
    for (final LocomotiveListener listener : listeners) {
      try {
        listener.circusTrainShutDown(completionCode, metrics);
      } catch (Exception e) {
        LOG.error("Listener '{}' threw exception on circusTrainShutDown.", listener, e);
      }
    }
  }

}
