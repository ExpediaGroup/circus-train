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
package com.hotels.bdp.circustrain.core.event;

import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hotels.bdp.circustrain.api.event.CopierListener;
import com.hotels.bdp.circustrain.api.metrics.Metrics;

public class CompositeCopierListener implements CopierListener {

  private static final Logger LOG = LoggerFactory.getLogger(CompositeCopierListener.class);

  private final List<CopierListener> listeners;

  public CompositeCopierListener(List<CopierListener> listeners) {
    this.listeners = Collections.unmodifiableList(listeners);
  }

  @Override
  public void copierStart(String copierImplementation) {
    for (final CopierListener listener : listeners) {
      try {
        listener.copierStart(copierImplementation);
      } catch (Exception e) {
        LOG.error("Listener '{}' threw exception on copierStart.", listener, e);
      }
    }
  }

  @Override
  public void copierEnd(Metrics metrics) {
    for (final CopierListener listener : listeners) {
      try {
        listener.copierEnd(metrics);
      } catch (Exception e) {
        LOG.error("Listener '{}' threw exception on copierEnd.", listener, e);
      }
    }
  }
}
