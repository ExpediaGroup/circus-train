/**
 * Copyright (C) 2016-2017 Expedia Inc and Apache Hadoop contributors.
 *
 * Based on {@code org.apache.hadoop.tools.util.TestDistCpUtils} from Hadoop DistCp 2.7.1:
 *
 * https://github.com/apache/hadoop/blob/release-2.7.1/hadoop-tools/hadoop-distcp/src/test/java/org/apache/hadoop/tools/util/TestDistCpUtils.java
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

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

import java.net.URI;

import org.apache.hadoop.fs.Path;
import org.junit.Test;

public class PathUtilTest {

  @Test
  public void getRelativePathRoot() {
    Path root = new Path("/");
    Path child = new Path("/a");
    assertThat(PathUtil.getRelativePath(root, child), is("/a"));
  }

  @Test
  public void relativePath() {
    Path root = new Path("/tmp/abc");
    Path child = new Path("/tmp/abc/xyz/file");
    assertThat(PathUtil.getRelativePath(root, child), is("/xyz/file"));
  }

  @Test
  public void nullPathToString() {
    assertThat(PathUtil.toString(null), is(nullValue()));
  }

  @Test
  public void pathToString() {
    assertThat(PathUtil.toString(new Path("/foo/bar/")), is("/foo/bar"));
    assertThat(PathUtil.toString(new Path("s3://bucket/foo/bar/")), is("s3://bucket/foo/bar"));
  }

  @Test(expected = NullPointerException.class)
  public void nullUriToBucketName() {
    PathUtil.toBucketName(null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void protocolOnlyUriToBucketName() {
    PathUtil.toBucketName(URI.create("s3://"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void uriWithNoAuthorityToBucketName() {
    PathUtil.toBucketName(URI.create("/foo/bar"));
  }

  @Test
  public void uriToBucketName() {
    assertThat(PathUtil.toBucketName(URI.create("s3://bucket/foo/bar")), is("bucket"));
  }

  @Test(expected = NullPointerException.class)
  public void nullUriToBucketKey() {
    PathUtil.toBucketKey(null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void protocolOnlyUriToBucketKey() {
    PathUtil.toBucketName(URI.create("s3://"));
  }

  @Test
  public void uriWithNoAuthorityToBucketKey() {
    assertThat(PathUtil.toBucketKey(URI.create("/foo/bar")), is("foo/bar"));
  }

  @Test
  public void uriToBucketKey() {
    assertThat(PathUtil.toBucketKey(URI.create("s3://bucket/foo/bar")), is("foo/bar"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void uriWithNoPathToBucketKey() {
    PathUtil.toBucketKey(URI.create("s3://bucket/"));
  }

  @Test
  public void isFile() {
    assertThat(PathUtil.isFile(URI.create("s3://bucket/foo/bar")), is(true));
    assertThat(PathUtil.isFile(URI.create("s3://bucket/foo/bar.txt")), is(true));
  }

  @Test
  public void isDirectory() {
    assertThat(PathUtil.isFile(URI.create("s3://bucket/foo/bar/")), is(false));
  }

}
