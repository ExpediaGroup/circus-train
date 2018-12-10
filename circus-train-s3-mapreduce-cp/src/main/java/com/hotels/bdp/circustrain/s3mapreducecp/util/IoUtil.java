/**
 * Copyright (C) 2016-2017 Expedia Inc and Apache Hadoop contributors.
 *
 * Based on {@code org.apache.hadoop.io.IOUtils} from Hadoop Common 2.7.1:
 *
 * https://github.com/apache/hadoop/blob/release-2.7.1/hadoop-common-project/hadoop-common/src/main/java/org/
 * apache/hadoop/io/IOUtils.java
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

import java.io.Closeable;

import org.slf4j.Logger;

/**
 * IO utility functions used in S3MapReduceCp.
 */
public final class IoUtil {

  private IoUtil() {}

  /**
   * Close the Closeable objects and <b>ignore</b> any {@link Throwable} or null pointers. Must only be used for cleanup
   * in exception handlers.
   *
   * @param closeables the objects to close
   */
  public static void closeSilently(Closeable... closeables) {
    closeSilently(null, closeables);
  }

  /**
   * Close the Closeable objects and <b>ignore</b> any {@link Throwable} or null pointers. Must only be used for cleanup
   * in exception handlers.
   * <p>
   * {@link Throwable} caught whilst closing a resource are logged out using the provided {@code logger}.
   * </p>
   *
   * @param log the log to record problems to at debug level. Can be null.
   * @param closeables the objects to close
   */
  public static void closeSilently(Logger log, Closeable... closeables) {
    for (Closeable c : closeables) {
      if (c != null) {
        try {
          c.close();
        } catch (Exception e) {
          if (log != null) {
            log.debug("Exception in closing {}", c, e);
          }
        }
      }
    }
  }

}
