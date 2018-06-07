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
  public void resolveAddsAuthorityFromRootPath() {
    setDfsPaths("hdp-ha");
    assertEquals("hdfs://hdp-ha/etl/test/avsc/schema.avsc",
        new FileSystemPathResolver(configuration).resolve("hdfs:/etl/test/avsc/schema.avsc").toString());
  }

  @Test
  public void resolveAddsAuthorityFromFullyQualifiedRootPath() {
    setDfsPaths("hdp-ha");
    assertEquals("hdfs://hdp-ha/etl/test/avsc/schema.avsc",
        new FileSystemPathResolver(configuration).resolve("hdfs:///etl/test/avsc/schema.avsc").toString());
  }

  @Test
  public void resolveAddsAuthorityAndSchemeWhenNoSchemeIsPresentAndDfsNameservicesIsSet() {
    setDfsPaths("foo");
    assertEquals("file://foo/etl/test/avsc/schema.avsc",
        new FileSystemPathResolver(configuration).resolve("/etl/test/avsc/schema.avsc").toString());
  }

  @Test
  public void resolveWithNoDfsNameservicesReturnsOriginalPathFromFullyQualifiedPath() {
    setDfsPaths("");
    Path path = new Path("hdfs:///etl/test/avsc/schema.avsc");
    assertEquals(path.toString(), new FileSystemPathResolver(configuration).resolve(path.toString()).toString());
  }

  @Test
  public void resolveWithNoDfsNameservicesReturnsOriginalPathFromPath() {
    setDfsPaths("");
    Path path = new Path("hdfs:/etl/test/avsc/schema.avsc");
    assertEquals(path.toString(), new FileSystemPathResolver(configuration).resolve(path.toString()).toString());
  }

  @Test
  public void resolveWithNoDfsNameservicesReturnsOriginalPathAndAddsSchemeWhenNoSchemeIsPresent() {
    setDfsPaths("");
    Path path = new Path("file:/etl/test/avsc/schema.avsc");
    assertEquals(path.toString(), new FileSystemPathResolver(configuration).resolve(path.toString()).toString());
  }

  @Test
  public void resolveWithNoDfsNameservicesReturnsOriginalPathFromFullyQualifiedRootPath() {
    Path path = new Path("hdfs:///etl/test/avsc/schema.avsc");
    assertEquals(path.toString(), new FileSystemPathResolver(configuration).resolve(path.toString()).toString());
  }

  @Test
  public void resolveWithNoDfsNameservicesReturnsOriginalPathFromRootPath() {
    Path path = new Path("hdfs:/etl/test/avsc/schema.avsc");
    assertEquals(path.toString(), new FileSystemPathResolver(configuration).resolve(path.toString()).toString());
  }

  @Test
  public void resolveWithNoDfsNameservicesReturnsOriginalPathFromPathWithScheme() {
    Path path = new Path("/etl/test/avsc/schema.avsc");
    assertEquals("file:/etl/test/avsc/schema.avsc", new FileSystemPathResolver(configuration).resolve(path.toString()).toString());
  }

  @Test
  public void resolveDoesntChangeSchemeIfSchemeIsSet() {
    Path path = new Path("s3:/etl/test/avsc/schema.avsc");
    assertEquals(path.toString(), new FileSystemPathResolver(configuration).resolve(path.toString()).toString());
  }

  @Test
  public void resolveDoesntChangeSchemeIfSchemeIsSetForFullyQualifiedPath() {
    Path path = new Path("s3:///etl/test/avsc/schema.avsc");
    assertEquals(path.toString(), new FileSystemPathResolver(configuration).resolve(path.toString()).toString());
  }

  private void setDfsPaths(String Path) {
    configuration.set(DFSConfigKeys.DFS_NAMESERVICES, Path);
  }
}
