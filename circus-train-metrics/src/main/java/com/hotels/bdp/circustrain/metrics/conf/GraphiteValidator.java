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
package com.hotels.bdp.circustrain.metrics.conf;

import org.apache.hadoop.conf.Configuration;

import com.hotels.bdp.circustrain.api.CircusTrainException;

public class GraphiteValidator {

  private final GraphiteLoader loader;

  public GraphiteValidator(Configuration conf) {
    this(new GraphiteLoader(conf));
  }

  GraphiteValidator(GraphiteLoader loader) {
    this.loader = loader;
  }

  public ValidatedGraphite validate(Graphite graphite) {
    if (graphite == null) {
      graphite = new Graphite();
    }

    Graphite validated = loader.load(graphite.getConfig());

    if (graphite.getHost() != null) {
      validated.setHost(graphite.getHost());
    }
    if (graphite.getPrefix() != null) {
      validated.setPrefix(graphite.getPrefix());
    }
    if (graphite.getNamespace() != null) {
      validated.setNamespace(graphite.getNamespace());
    }
    validated.init();

    if (validated.isEnabled()) {
      if (validated.getHost() == null || validated.getPrefix() == null || validated.getNamespace() == null) {
        throw new CircusTrainException(
            String.format("Missing graphite configuration property: host[%s],  prefix[%s], namespace[%s]",
                validated.getHost(), validated.getPrefix(), validated.getNamespace()));
      }
    }

    return new ValidatedGraphite(validated);
  }

}
