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
package com.hotels.bdp.circustrain.avro.util;

import static org.apache.commons.lang.StringUtils.isBlank;
import static org.apache.commons.lang.StringUtils.isNotBlank;
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

  private static final String HDFS_SCHEME = "hdfs";

  private final Configuration configuration;

  public FileSystemPathResolver(Configuration configuration) {
    this.configuration = configuration;
  }

  public Path resolveScheme(Path path) {
    try {
      URI uri = path.toUri();
      if (isEmpty(uri.getScheme())) {
        String scheme = FileSystem.get(configuration).getScheme();
        Path result = new Path(new URI(scheme, uri.getUserInfo(), uri.getHost(), uri.getPort(), uri.getPath(),
            uri.getQuery(), uri.getFragment()).toString());
        LOG.info("Added scheme {} to path {}. Resulting path is {}", scheme, path, result);
        return result;
      }
    } catch (URISyntaxException | IOException e) {
      throw new CircusTrainException(e);
    }
    return path;
  }

  public Path resolveNameServices(Path path) {
    if (HDFS_SCHEME.equalsIgnoreCase(path.toUri().getScheme())) {
      String nameService = configuration.get(DFSConfigKeys.DFS_NAMESERVICES);
      if (isNotBlank(nameService)) {
        URI uri = path.toUri();
        String scheme = uri.getScheme();
        String url = uri.getPath();
        final String original = path.toString();
        if (isBlank(scheme)) {
          url = String.format("/%s%s", nameService, path);
          path = new Path(url);
        } else {
          path = new Path(scheme, nameService, url);
        }
        LOG.info("Added nameservice to path. {} became {}", original, path);
      }
    }
    return path;
  }

}
