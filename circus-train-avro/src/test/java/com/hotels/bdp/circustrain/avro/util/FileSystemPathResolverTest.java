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
 * With WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hotels.bdp.circustrain.avro.util;

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.junit.Test;

public class FileSystemPathResolverTest {

  private Configuration configuration = new Configuration();

  @Test
  public void resolvePathAddsAuthorityFromRootPath() {
    setDfsPaths("hdp-ha");
    assertEquals("hdfs://hdp-ha/etl/test/avsc/schema.avsc",
        new FileSystemPathResolver(configuration).resolve("hdfs:/etl/test/avsc/schema.avsc").toString());
  }

  @Test
  public void resolvePathAddsAuthorityFromFullyQualifiedRootPath() {
    setDfsPaths("hdp-ha");
    assertEquals("hdfs://hdp-ha/etl/test/avsc/schema.avsc",
        new FileSystemPathResolver(configuration).resolve("hdfs:///etl/test/avsc/schema.avsc").toString());
  }

  @Test
  public void resolvePathAddsAuthorityFromPathWithScheme() {
    setDfsPaths("foo");
    assertEquals("file://foo/etl/test/avsc/schema.avsc",
        new FileSystemPathResolver(configuration).resolve("/etl/test/avsc/schema.avsc").toString());
  }

  @Test
  public void resolveEmptyPathReturnsOriginalPathFromFullyQualifiedRootPath() {
    setDfsPaths("");
    Path path = new Path("hdfs:///etl/test/avsc/schema.avsc");
    assertEquals(path.toString(), new FileSystemPathResolver(configuration).resolve(path.toString()).toString());
  }

  @Test
  public void resolveEmptyPathReturnsOriginalPathFromRootPath() {
    setDfsPaths("");
    Path path = new Path("hdfs:/etl/test/avsc/schema.avsc");
    assertEquals(path.toString(), new FileSystemPathResolver(configuration).resolve(path.toString()).toString());
  }

  @Test
  public void resolveEmptyPathReturnsOriginalPathFromPathWithScheme() {
    setDfsPaths("");
    Path path = new Path("file:/etl/test/avsc/schema.avsc");
    assertEquals(path.toString(), new FileSystemPathResolver(configuration).resolve(path.toString()).toString());
  }

  @Test
  public void resolveNullPathReturnsOriginalPathFromFullyQualifiedRootPath() {
    Path path = new Path("hdfs:///etl/test/avsc/schema.avsc");
    assertEquals(path.toString(), new FileSystemPathResolver(configuration).resolve(path.toString()).toString());
  }

  @Test
  public void resolveNullPathReturnsOriginalPathFromRootPath() {
    Path path = new Path("hdfs:/etl/test/avsc/schema.avsc");
    assertEquals(path.toString(), new FileSystemPathResolver(configuration).resolve(path.toString()).toString());
  }

  @Test
  public void resolveNullPathReturnsOriginalPathFromPathWithScheme() {
    Path path = new Path("/etl/test/avsc/schema.avsc");
    assertEquals("file:/etl/test/avsc/schema.avsc", new FileSystemPathResolver(configuration).resolve(path.toString()).toString());
  }

  private void setDfsPaths(String Path) {
    configuration.set(DFSConfigKeys.DFS_NAMESERVICES, Path);
  }
}
