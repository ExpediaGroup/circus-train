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

import org.apache.hadoop.fs.Path;
import org.junit.Test;

public class NameServicePathResolverTest {

  @Test
  public void resolveNameServiceAddsAuthorityFromRootPath() {
    assertEquals("hdfs://hdp-ha/etl/test/avsc0/tlog.avsc",
        new NameServicePathResolver("hdfs:/etl/test/avsc0/tlog.avsc", "hdp-ha").resolve().toString());
  }

  @Test
  public void resolveNameServiceAddsAuthorityFromFullyQualifiedRootPath() {
    assertEquals("hdfs://hdp-ha/etl/test/avsc0/tlog.avsc",
        new NameServicePathResolver("hdfs:///etl/test/avsc0/tlog.avsc", "hdp-ha").resolve().toString());
  }

  @Test
  public void resolveNameServiceAddsAuthorityFromPathWithoutScheme() {
    assertEquals("/hdp-ha/etl/test/avsc0/tlog.avsc",
        new NameServicePathResolver("/etl/test/avsc0/tlog.avsc", "hdp-ha").resolve().toString());
  }

  @Test
  public void resolveNullNameServiceReturnsOriginalPathFromFullyQualifiedRootPath() {
    Path path = new Path("hdfs:///etl/test/avsc0/tlog.avsc");
    assertEquals(path.toString(), new NameServicePathResolver(path.toString(), null).resolve().toString());
  }

  @Test
  public void resolveNullNameServiceReturnsOriginalPathFromRootPath() {
    Path path = new Path("hdfs:/etl/test/avsc0/tlog.avsc");
    assertEquals(path.toString(), new NameServicePathResolver(path.toString(), null).resolve().toString());
  }

  @Test
  public void resolveNullNameServiceReturnsOriginalPathFromPathWithoutScheme() {
    Path path = new Path("/etl/test/avsc0/tlog.avsc");
    assertEquals(path.toString(), new NameServicePathResolver(path.toString(), null).resolve().toString());
  }

  @Test
  public void resolveEmptyNameServiceReturnsOriginalPathFromFullyQualifiedRootPath() {
    Path path = new Path("hdfs:///etl/test/avsc0/tlog.avsc");
    assertEquals(path.toString(), new NameServicePathResolver(path.toString(), "").resolve().toString());
  }

  @Test
  public void resolveEmptyNameServiceReturnsOriginalPathFromRootPath() {
    Path path = new Path("hdfs:/etl/test/avsc0/tlog.avsc");
    assertEquals(path.toString(), new NameServicePathResolver(path.toString(), "").resolve().toString());
  }

  @Test
  public void resolveEmptyNameServiceReturnsOriginalPathFromPathWithoutScheme() {
    Path path = new Path("/etl/test/avsc0/tlog.avsc");
    assertEquals(path.toString(), new NameServicePathResolver(path.toString(), "").resolve().toString());
  }
}
