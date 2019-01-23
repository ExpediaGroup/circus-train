/**
 * Copyright (C) 2016-2019 Expedia Inc.
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
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.hadoop.fs.Path;

import com.google.common.base.Function;
import com.google.common.base.Splitter;
import com.google.common.collect.FluentIterable;

public final class MoreMapUtils {

  private MoreMapUtils() {}

  private static <T> List<T> getList(
      Map<?, ?> map,
      Object key,
      List<T> defaultValue,
      Function<Object, T> transformation) {
    Object value = map.get(key);
    if (value == null) {
      return defaultValue;
    }

    if (value instanceof String) {
      value = Splitter.on(',').splitToList(value.toString());
    }

    if (!(value instanceof Collection)) {
      return Collections.singletonList(transformation.apply(value));
    }

    return FluentIterable.from((Collection<?>) value).transform(transformation).toList();
  }

  public static <T extends Enum<T>> List<T> getListOfEnum(
      Map<?, ?> map,
      Object key,
      List<T> defaultValue,
      final Class<T> enumClass) {
    return getList(map, key, defaultValue, new Function<Object, T>() {
      @Override
      public T apply(Object input) {
        if (input == null) {
          return null;
        }
        if (enumClass.isAssignableFrom(input.getClass())) {
          return enumClass.cast(input);
        }
        return Enum.valueOf(enumClass, input.toString().trim().toUpperCase(Locale.ROOT));
      }
    });
  }

  public static Path getHadoopPath(Map<?, ?> map, Object key, Path defaultValue) {
    Object path = map.get(key);
    if (path == null) {
      return defaultValue;
    }
    if (path instanceof Path) {
      return (Path) path;
    }
    if (path instanceof String) {
      return new Path(((String) path).trim());
    }
    throw new IllegalArgumentException(
        "Object '" + key + "' must be a String or a Path. Got " + path.getClass().getName());
  }

  public static URI getUri(Map<?, ?> map, Object key, URI defaultValue) {
    Object uri = map.get(key);
    if (uri == null) {
      return defaultValue;
    }
    if (uri instanceof URI) {
      return (URI) uri;
    }
    if (uri instanceof String) {
      try {
        return URI.create(((String) uri).trim());
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException("Object '" + key + "' is not a valid URI: " + uri, e);
      }
    }
    throw new IllegalArgumentException(
        "Object '" + key + "' must be a String or a URI. Got " + uri.getClass().getName());
  }

}
