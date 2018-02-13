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
package com.hotels.bdp.circustrain.core.util;

import java.net.URI;
import java.net.URISyntaxException;

/** Resolves the {@link URI} for the JAR file that contains the specified class. */
public interface JarPathResolver {

  public static JarPathResolver PROTECTION_DOMAIN = new JarPathResolver() {
    @Override
    public URI getPath(Class<?> classInJarFile) {
      try {
        return classInJarFile.getProtectionDomain().getCodeSource().getLocation().toURI();
      } catch (URISyntaxException e) {
        throw new RuntimeException(e);
      }
    }
  };

  URI getPath(Class<?> classInJarFile);

}
