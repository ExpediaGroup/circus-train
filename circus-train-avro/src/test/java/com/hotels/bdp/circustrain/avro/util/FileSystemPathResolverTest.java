/**
 * Copyright (C) 2016-2019 Expedia, Inc.
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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.junit.Test;

public class FileSystemPathResolverTest {

  private final Configuration configuration = new Configuration();
  private final FileSystemPathResolver resolver = new FileSystemPathResolver(configuration);

  @Test
  public void resolveSchemeDoesntChangeSchemeForPathWithScheme() {
    Path expected = new Path("s3:/etl/test/avsc/schema.avsc");
    Path result = resolver.resolveScheme(expected);
    assertThat(result, is(expected));
  }

  @Test
  public void resolveSchemeDoesntChangeSchemeForFullyQualifiedPathWithScheme() {
    Path expected = new Path("s3:///etl/test/avsc/schema.avsc");
    Path result = resolver.resolveScheme(expected);
    assertThat(result, is(expected));
  }

  @Test
  public void resolveSchemeSetsSchemeIfSchemeIsntPresent() {
    Path input = new Path("/etl/test/avsc/schema.avsc");
    Path result = resolver.resolveScheme(input);
    Path expected = new Path("file:/etl/test/avsc/schema.avsc");
    assertThat(result, is(expected));
  }

  @Test
  public void resolveNameServiceAddsAuthorityToPathWithScheme() {
    setDfsPaths("hdp-ha");
    Path input = new Path("hdfs:/etl/test/avsc/schema.avsc");
    Path result = resolver.resolveNameServices(input);
    Path expected = new Path("hdfs://hdp-ha/etl/test/avsc/schema.avsc");
    assertThat(result, is(expected));
  }

  @Test
  public void resolveNameServiceS3() {
    setDfsPaths("hdp-ha");
    Path input = new Path("s3:///etl/test/avsc/schema.avsc");
    Path result = resolver.resolveNameServices(input);
    assertThat(result, is(input));
  }

  @Test
  public void resolveNameServicesAddsAuthorityToFullyQualifiedPathWithScheme() {
    setDfsPaths("hdp-ha");
    Path input = new Path("hdfs:///etl/test/avsc/schema.avsc");
    Path result = resolver.resolveNameServices(input);
    Path expected = new Path("hdfs://hdp-ha/etl/test/avsc/schema.avsc");
    assertThat(result, is(expected));
  }

  @Test
  public void resolveNameServicesAddsAuthorityToPathWithoutScheme() {
    setDfsPaths("foo");
    Path input = new Path("/etl/test/avsc/schema.avsc");
    Path result = resolver.resolveNameServices(input);
    Path expected = new Path("/etl/test/avsc/schema.avsc");
    assertThat(result, is(expected));
  }

  @Test
  public void resolveNameServicesWithEmptyDfsNameservicesConfiguredReturnsOriginalPathFromFullyQualifiedPathWithScheme() {
    setDfsPaths("");
    Path expected = new Path("hdfs:///etl/test/avsc/schema.avsc");
    Path result = resolver.resolveNameServices(expected);
    assertThat(result, is(expected));
  }

  @Test
  public void resolveNameServicesWithEmptyDfsNameservicesConfiguredReturnsOriginalPathFromPathWithScheme() {
    setDfsPaths("");
    Path expected = new Path("hdfs:/etl/test/avsc/schema.avsc");
    Path result = resolver.resolveNameServices(expected);
    assertThat(result, is(expected));
  }

  @Test
  public void resolveNameServicesWithoutDfsNameservicesConfiguredReturnsOriginalPathFromFullyQualifiedPathWithScheme() {
    Path expected = new Path("hdfs:///etl/test/avsc/schema.avsc");
    Path result = resolver.resolveNameServices(expected);
    assertThat(result, is(expected));
  }

  @Test
  public void resolveNameServicesWithoutDfsNameservicesConfiguredReturnsOriginalPathFromPathWithScheme() {
    Path expected = new Path("hdfs:/etl/test/avsc/schema.avsc");
    Path result = resolver.resolveNameServices(expected);
    assertThat(result, is(expected));
  }

  @Test
  public void resolveNameServicesWithoutDfsNameservicesReturnsOriginalPathWithoutScheme() {
    Path expected = new Path("/etl/test/avsc/schema.avsc");
    Path result = resolver.resolveNameServices(expected);
    assertThat(result, is(expected));
  }

  private void setDfsPaths(String Path) {
    configuration.set(DFSConfigKeys.DFS_NAMESERVICES, Path);
  }
}
