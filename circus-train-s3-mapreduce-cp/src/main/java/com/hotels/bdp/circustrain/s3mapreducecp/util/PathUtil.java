/**
 * Copyright (C) 2016-2017 Expedia Inc and Apache Hadoop contributors.
 *
 * Based on {@code org.apache.hadoop.tools.util.DistCpUtils} from Hadoop DistCp 2.7.1:
 *
 * https://github.com/apache/hadoop/blob/release-2.7.1/hadoop-tools/hadoop-distcp/src/main/java/org/
 * apache/hadoop/tools/util/DistCpUtils.java
 *
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
package com.hotels.bdp.circustrain.s3mapreducecp.util;

import java.net.URI;

import org.apache.hadoop.fs.Path;

/**
 * Utility Path functions used in S3MapReduceCp.
 */
public final class PathUtil {

  private PathUtil() {}

  /**
   * Gets relative path of child path with respect to a root path. For example, if {@code childPath=/tmp/abc/xyz/file}
   * and {@code sourceRootPath=/tmp/abc} relative path would be {@code /xyz/file}. If {@code childPath=/file} and
   * {@code sourceRootPath=/} relative path would be {@code /file}.
   *
   * @param sourceRootPath - Source root path
   * @param childPath - Path for which relative path is required
   * @return - Relative portion of the child path (always prefixed with / unless it is empty
   */
  public static String getRelativePath(Path sourceRootPath, Path childPath) {
    String childPathString = childPath.toUri().getPath();
    String sourceRootPathString = sourceRootPath.toUri().getPath();
    return "/".equals(sourceRootPathString) ? childPathString
        : childPathString.substring(sourceRootPathString.length());
  }

  /**
   * Converts a {@code Path} to its {@code String} representation.
   *
   * @param path Path to convert
   * @return {@code null} if {@code path} is {@code null}, otherwise the {@code String} representation of the given
   *         {@code path}
   */
  public static String toString(Path path) {
    return path == null ? null : path.toUri().toString();
  }

  /**
   * Extracts the S3 bucket name from the given {@code URI}.
   *
   * @param uri URI
   * @return Bucket name
   */
  public static String toBucketName(URI uri) {
    String authority = uri.getAuthority();
    if (authority == null) {
      throw new IllegalArgumentException("URI " + uri + " has no authority part");
    }
    return authority;
  }

  /**
   * Extracts the S3 object key from the given {@code URI}.
   *
   * @param uri URI
   * @return Object key
   */
  public static String toBucketKey(URI uri) {
    String key = uri.getPath().substring(1);
    if (key.isEmpty()) {
      throw new IllegalArgumentException("URI " + uri + " has no path part");
    }
    return key;
  }

  public static boolean isFile(URI path) {
    return !path.toString().endsWith("/");
  }

}
