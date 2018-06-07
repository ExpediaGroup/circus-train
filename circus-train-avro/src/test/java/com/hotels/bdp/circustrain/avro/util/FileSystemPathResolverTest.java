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

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.junit.Test;

public class FileSystemPathResolverTest {

  private Configuration configuration = new Configuration();

  @Test
  public void resolveSchemeDoesntChangeSchemeForPathWithScheme() {
    Path path = new Path("s3:/etl/test/avsc/schema.avsc");
    assertEquals(path, new FileSystemPathResolver(configuration).resolveScheme(path));
  }

  @Test
  public void resolveSchemeDoesntChangeSchemeForFullyQualifiedPathWithScheme() {
    Path path = new Path("s3:///etl/test/avsc/schema.avsc");
    assertEquals(path, new FileSystemPathResolver(configuration).resolveScheme(path.toString()));
  }

  @Test
  public void resolveSchemeSetsSchemeIfSchemeIsntPresent() {
    Path path = new Path("/etl/test/avsc/schema.avsc");
    assertEquals("file:/etl/test/avsc/schema.avsc",
        new FileSystemPathResolver(configuration).resolveScheme(path).toString());
  }

  @Test
  public void resolveNameServiceAddsAuthorityToPathWithScheme() {
    setDfsPaths("hdp-ha");
    assertEquals("hdfs://hdp-ha/etl/test/avsc/schema.avsc",
        new FileSystemPathResolver(configuration).resolveNameServices("hdfs:/etl/test/avsc/schema.avsc").toString());
  }

  @Test
  public void resolveNameServicesAddsAuthorityToFullyQualifiedPathWithScheme() {
    setDfsPaths("hdp-ha");
    assertEquals("hdfs://hdp-ha/etl/test/avsc/schema.avsc",
        new FileSystemPathResolver(configuration).resolveNameServices("hdfs:///etl/test/avsc/schema.avsc").toString());
  }

  @Test
  public void resolveNameServicesAddsAuthorityToPathWithoutScheme() {
    setDfsPaths("foo");
    assertEquals("/foo/etl/test/avsc/schema.avsc",
        new FileSystemPathResolver(configuration).resolveNameServices("/etl/test/avsc/schema.avsc").toString());
  }

  @Test
  public void resolveNameServicesWithEmptyDfsNameservicesConfiguredReturnsOriginalPathFromFullyQualifiedPathWithScheme() {
    setDfsPaths("");
    Path path = new Path("hdfs:///etl/test/avsc/schema.avsc");
    assertEquals(path.toString(),
        new FileSystemPathResolver(configuration).resolveNameServices(path.toString()).toString());
  }

  @Test
  public void resolveNameServicesWithEmptyDfsNameservicesConfiguredReturnsOriginalPathFromPathWithScheme() {
    setDfsPaths("");
    Path path = new Path("hdfs:/etl/test/avsc/schema.avsc");
    assertEquals(path.toString(),
        new FileSystemPathResolver(configuration).resolveNameServices(path.toString()).toString());
  }

  @Test
  public void resolveNameServicesWithoutDfsNameservicesConfiguredReturnsOriginalPathFromFullyQualifiedPathWithScheme() {
    Path path = new Path("hdfs:///etl/test/avsc/schema.avsc");
    assertEquals(path.toString(),
        new FileSystemPathResolver(configuration).resolveNameServices(path.toString()).toString());
  }

  @Test
  public void resolveNameServicesWithoutDfsNameservicesConfiguredReturnsOriginalPathFromPathWithScheme() {
    Path path = new Path("hdfs:/etl/test/avsc/schema.avsc");
    assertEquals(path.toString(),
        new FileSystemPathResolver(configuration).resolveNameServices(path.toString()).toString());
  }

  @Test
  public void resolveNameServicesWithoutDfsNameservicesReturnsOriginalPathWithoutScheme() {
    Path path = new Path("/etl/test/avsc/schema.avsc");
    assertEquals("/etl/test/avsc/schema.avsc",
        new FileSystemPathResolver(configuration).resolveNameServices(path.toString()).toString());
  }

  private void setDfsPaths(String Path) {
    configuration.set(DFSConfigKeys.DFS_NAMESERVICES, Path);
  }
}
