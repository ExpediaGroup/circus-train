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
package com.hotels.bdp.circustrain.core.metastore;

import java.net.URI;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;

import com.google.common.base.Strings;

/**
 * On hadoop {@link Path} objects:
 * <p>
 * A {@link Path} fetched from the hive metastore are assumed stored encoded e.g.:
 *
 * <pre>
 * table.getSd().getLocation()
 * partition.getSd().getLocation()
 * Will have url encoded string, e.g. /folder/a_percent_%25_encoded/
 * </pre>
 *
 * Strings that are encoded can be turned into a Path like this: <code>new Path(String)</code>. Calling
 * <code>new Path(String).toUri()</code> on such a path will return a double encoded URI.
 * <p>
 * Calling <code>new Path(String).toString()</code> on such a path will return the correct path representation(single
 * encoded).
 * <p>
 * The Hadoop {@link DistributedFileSystem} expects to have a double encoded URI as it does
 * <code>path.toURI().getPath()</code> to get the unencoded path (without scheme) back. For all our purposes in Circus
 * Train code we can always create Paths with the <code>new Path(String)</code> constructor. We can get the string
 * representation back by calling <code>path.toString()</code>.
 */
public final class LocationUtils {

  private LocationUtils() {}

  /**
   * @param table
   * @return {@code true} if the {@code table} has location, {@code false} otherwise
   */
  public static boolean hasLocation(Table table) {
    return table.getSd() != null && !Strings.isNullOrEmpty(table.getSd().getLocation());
  }

  /**
   * @param partition
   * @return {@code true} if the {@code partition} has location, {@code false} otherwise
   */
  public static boolean hasLocation(Partition partition) {
    return partition.getSd() != null && !Strings.isNullOrEmpty(partition.getSd().getLocation());
  }

  /**
   * @param table
   * @return the table.getSd().getLocation() as a {@link URI} (correctly URL encoded)
   */
  public static URI locationAsUri(Table table) {
    return locationAsPath(table).toUri();
  }

  /**
   * @param partition
   * @return the partition.getSd().getLocation() as a {@link URI} (correctly URL encoded)
   */
  public static URI locationAsUri(Partition partition) {
    return locationAsPath(partition).toUri();
  }

  /**
   * @param table
   * @return the table.getSd().getLocation() as a {@link Path} (correctly URL encoded)
   */
  public static Path locationAsPath(Table table) {
    return new Path(table.getSd().getLocation());
  }

  /**
   * @param partition
   * @return the partition.getSd().getLocation() as a {@link Path} (correctly URL encoded)
   */
  public static Path locationAsPath(Partition partition) {
    return new Path(partition.getSd().getLocation());
  }
}
