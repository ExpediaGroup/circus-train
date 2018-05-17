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

import java.net.URI;

import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NameServicePathResolver {
  private static final Logger LOG = LoggerFactory.getLogger(NameServicePathResolver.class);

  private final String url;
  private final String nameService;

  public NameServicePathResolver(String url, String nameService) {
    this.url = url;
    this.nameService = nameService;
  }

  public Path resolve() {
    Path location;
    if (isBlank(nameService)) {
      location = new Path(url);
    } else {
      URI uri = URI.create(url);
      String scheme = uri.getScheme();
      String path = uri.getPath();
      if (isBlank(scheme)) {
        path = "/" + nameService + path;
        location = new Path(path);
      } else {
        location = new Path(scheme, nameService, path);
      }
      LOG.info("Added nameservice to path. {} became {}", url, location.toString());
    }
    return location;
  }
}
