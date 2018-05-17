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

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class SchemaCopierTest {

  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  private SchemaCopier copier = new SchemaCopier(new HiveConf(), new HiveConf());

  @Test
  public void copiedToCorrectDestination() throws IOException {
    File source = temporaryFolder.newFile("test.txt");
    File destination = temporaryFolder.newFolder();
    copier.copy(source.toString(), destination.toString());
    FileSystem fs = new Path(destination.toString()).getFileSystem(new HiveConf());
    assertTrue(fs.exists(new Path(destination.toString() + "/test.txt")));
  }

  @Test
  public void copiedCorrectFile() throws IOException {
    List<String> randomData = new ArrayList<>();
    randomData.add("foo");
    randomData.add("baz");
    File source = temporaryFolder.newFile("test.txt");
    FileUtils.writeLines(source, randomData);
    File destination = temporaryFolder.newFolder();
    File copy = new File(copier.copy(source.toString(), destination.toString()).toString());
    assertTrue(FileUtils.contentEquals(source, copy));
  }

  @Test
  public void copyDoesntDeleteOriginalFile() throws IOException {
    File source = temporaryFolder.newFile("test.txt");
    File destination = temporaryFolder.newFolder();
    copier.copy(source.toString(), destination.toString());
    FileSystem fs = new Path(destination.toString()).getFileSystem(new HiveConf());
    assertTrue(fs.exists(new Path(source.toString())));
  }

  @Test
  public void copiedFiltestdNotDirectory() throws IOException {
    File source = temporaryFolder.newFile("test.txt");
    File destination = temporaryFolder.newFolder();
    copier.copy(source.toString(), destination.toString());
    assertTrue(new File(destination.toString() + "/test.txt").isFile());
  }

  @Test(expected = NullPointerException.class)
  public void copyWithNullSourceParamThrowsException() throws IOException {
    File destination = temporaryFolder.newFolder();
    copier.copy(null, destination.toString());
  }

  @Test(expected = IllegalArgumentException.class)
  public void copyWithEmptySourceParamThrowsException() throws IOException {
    File destination = temporaryFolder.newFolder();
    copier.copy("", destination.toString());
  }

  @Test(expected = NullPointerException.class)
  public void copyWithNullDestinationParamThrowsException() throws IOException {
    File source = temporaryFolder.newFile("test.txt");
    copier.copy(source.toString(), null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void copyWithEmptyDestinationParamThrowsException() throws IOException {
    File source = temporaryFolder.newFile("test.txt");
    copier.copy(source.toString(), "");
  }
}
