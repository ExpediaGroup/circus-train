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
package com.hotels.bdp.circustrain.tool.vacuum;

import java.io.IOException;
import java.util.Collections;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class ConsistencyCheckTest {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test(expected = IllegalStateException.class)
  public void fsPathNoEventId() {
    ConsistencyCheck.checkFsPath(new Path("/a/b/c/d"));
  }

  @Test
  public void fsPathWithEventId() {
    ConsistencyCheck.checkFsPath(new Path("/a/b/ctp-20160728T110821.830Z-w5npK1yY/d"));
  }

  @Test
  public void metastorePathsCorrect() {
    ConsistencyCheck.checkMetastorePaths(Collections.singleton(new Path("/a/b/ctp-20160728T110821.830Z-w5npK1yY/d")),
        4);
  }

  @Test(expected = IllegalStateException.class)
  public void metastorePathsDepthIncorrect() {
    ConsistencyCheck.checkMetastorePaths(Collections.singleton(new Path("/a/ctp-20160728T110821.830Z-w5npK1yY/d")), 4);
  }

  @Test(expected = IllegalStateException.class)
  public void metastorePathsNoEventId() {
    ConsistencyCheck.checkMetastorePaths(Collections.singleton(new Path("/a/b/c/d")), 4);
  }

  @Test
  public void metastorePathCorrect() {
    ConsistencyCheck.checkMetastorePath(new Path("/a/b/ctp-20160728T110821.830Z-w5npK1yY/d"), 4);
  }

  @Test(expected = IllegalStateException.class)
  public void metastorePathDepthIncorrect() {
    ConsistencyCheck.checkMetastorePath(new Path("/a/ctp-20160728T110821.830Z-w5npK1yY/d"), 4);
  }

  @Test(expected = IllegalStateException.class)
  public void metastorePathNoEventId() {
    ConsistencyCheck.checkMetastorePath(new Path("/a/b/c/d"), 4);
  }

  @Test
  public void unvisitedPath() throws IOException {
    Path nonExistent = new Path("/a/b/c/" + RandomStringUtils.randomAlphanumeric(8) + "/d");
    FileSystem fs = nonExistent.getFileSystem(new Configuration(false));
    ConsistencyCheck.checkUnvisitedPath(fs, nonExistent);
  }

  @Test(expected = IllegalStateException.class)
  public void unvisitedPathExists() throws IOException {
    Path exists = new Path(temporaryFolder.newFolder("a", "b", RandomStringUtils.randomAlphanumeric(8)).toURI());
    FileSystem fs = exists.getFileSystem(new Configuration(false));
    ConsistencyCheck.checkUnvisitedPath(fs, exists);
  }

}
