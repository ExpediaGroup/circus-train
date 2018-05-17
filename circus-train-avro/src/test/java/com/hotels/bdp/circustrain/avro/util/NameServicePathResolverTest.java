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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class NameServicePathResolverTest {

  private Configuration configuration = new Configuration();

  @Test
  public void resolveNameServiceAddsAuthorityFromRootPath() {
    setDfsNameservices("hdp-ha");
    assertEquals("hdfs://hdp-ha/etl/test/avsc0/tlog.avsc",
        new NameServicePathResolver(configuration).resolve("hdfs:/etl/test/avsc0/tlog.avsc").toString());
  }

  @Test
  public void resolveNameServiceAddsAuthorityFromFullyQualifiedRootPath() {
    setDfsNameservices("hdp-ha");
    assertEquals("hdfs://hdp-ha/etl/test/avsc0/tlog.avsc",
        new NameServicePathResolver(configuration).resolve("hdfs:///etl/test/avsc0/tlog.avsc").toString());
  }

  @Test
  public void resolveNameServiceAddsAuthorityFromPathWithoutScheme() {
    setDfsNameservices("hdp-ha");
    assertEquals("/hdp-ha/etl/test/avsc0/tlog.avsc",
        new NameServicePathResolver(configuration).resolve("/etl/test/avsc0/tlog.avsc").toString());
  }

  @Test
  public void resolveEmptyNameServiceReturnsOriginalPathFromFullyQualifiedRootPath() {
    setDfsNameservices("");
    Path path = new Path("hdfs:///etl/test/avsc0/tlog.avsc");
    assertEquals(path.toString(), new NameServicePathResolver(configuration).resolve(path.toString()).toString());
  }

  @Test
  public void resolveEmptyNameServiceReturnsOriginalPathFromRootPath() {
    setDfsNameservices("");
    Path path = new Path("hdfs:/etl/test/avsc0/tlog.avsc");
    assertEquals(path.toString(), new NameServicePathResolver(configuration).resolve(path.toString()).toString());
  }

  @Test
  public void resolveEmptyNameServiceReturnsOriginalPathFromPathWithoutScheme() {
    setDfsNameservices("");
    Path path = new Path("/etl/test/avsc0/tlog.avsc");
    assertEquals(path.toString(), new NameServicePathResolver(configuration).resolve(path.toString()).toString());
  }

  @Test
  public void resolveNullNameServiceReturnsOriginalPathFromFullyQualifiedRootPath() {
    Path path = new Path("hdfs:///etl/test/avsc0/tlog.avsc");
    assertEquals(path.toString(), new NameServicePathResolver(configuration).resolve(path.toString()).toString());
  }

  @Test
  public void resolveNullNameServiceReturnsOriginalPathFromRootPath() {
    Path path = new Path("hdfs:/etl/test/avsc0/tlog.avsc");
    assertEquals(path.toString(), new NameServicePathResolver(configuration).resolve(path.toString()).toString());
  }

  @Test
  public void resolveNullNameServiceReturnsOriginalPathFromPathWithoutScheme() {
    Path path = new Path("/etl/test/avsc0/tlog.avsc");
    assertEquals(path.toString(), new NameServicePathResolver(configuration).resolve(path.toString()).toString());
  }

  private void setDfsNameservices(String nameservice) {
    configuration.set(DFSConfigKeys.DFS_NAMESERVICES, nameservice);
  }
}
