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

package com.hotels.bdp.circustrain.avro.util;

import static org.apache.commons.lang.StringUtils.isBlank;
import static org.springframework.util.StringUtils.isEmpty;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hotels.bdp.circustrain.api.CircusTrainException;

public class FileSystemPathResolver {
  private static final Logger LOG = LoggerFactory.getLogger(FileSystemPathResolver.class);

  private final Configuration configuration;

  public FileSystemPathResolver(Configuration configuration) {
    this.configuration = configuration;
  }

  public Path resolve(String url) {
    try {
      URI uri = new URI(url);
      if (isEmpty(uri.getScheme())) {
        String scheme = FileSystem.get(configuration).getScheme();
        url = new URI(scheme, uri.getUserInfo(), uri.getHost(), uri.getPort(), uri.getPath(), uri.getQuery(),
            uri.getFragment()).toString();
      }
    } catch (URISyntaxException | IOException e) {
      throw new CircusTrainException(e);
    }

    String nameService = configuration.get(DFSConfigKeys.DFS_NAMESERVICES);
    Path location;
    if (isBlank(nameService)) {
      location = new Path(url);
    } else {
      URI uri = URI.create(url);
      String scheme = uri.getScheme();
      String path = uri.getPath();
      if (isBlank(scheme)) {
        path = String.format("/%s%s", nameService, path);
        location = new Path(path);
      } else {
        location = new Path(scheme, nameService, path);
      }
      LOG.info("Added nameservice to path. {} became {}", url, location.toString());
    }
    return location;
  }
}
