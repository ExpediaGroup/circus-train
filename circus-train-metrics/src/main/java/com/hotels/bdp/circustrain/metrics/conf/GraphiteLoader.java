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

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.hotels.bdp.circustrain.api.CircusTrainException;

class GraphiteLoader {

  private final Configuration conf;

  GraphiteLoader(Configuration conf) {
    this.conf = conf;
  }

  Graphite load(Path path) {
    Graphite graphite = new Graphite();
    if (path != null) {
      graphite.setConfig(path);
      Properties properties = loadProperties(path);
      graphite.setHost(properties.getProperty("graphite.host"));
      graphite.setPrefix(properties.getProperty("graphite.prefix"));
      graphite.setNamespace(properties.getProperty("graphite.namespace"));
    }
    return graphite;
  }

  private Properties loadProperties(Path path) {
    try {
      FileSystem fileSystem = path.getFileSystem(conf);
      if (!fileSystem.exists(path)) {
        throw new CircusTrainException("Graphite configuration file does not exist: " + path);
      }
      Properties properties = new Properties();
      try (InputStream inputStream = fileSystem.open(path)) {
        properties.load(inputStream);
      }
      return properties;
    } catch (IOException e) {
      throw new CircusTrainException("Unable to load configuration file: " + path, e);
    }
  }

}
