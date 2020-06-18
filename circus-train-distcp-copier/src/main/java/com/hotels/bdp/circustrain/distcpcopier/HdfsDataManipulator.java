/**
 * Copyright (C) 2016-2020 Expedia, Inc.
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
package com.hotels.bdp.circustrain.distcpcopier;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hotels.bdp.circustrain.api.data.DataManipulator;

public class HdfsDataManipulator implements DataManipulator {

  private static final Logger log = LoggerFactory.getLogger(HdfsDataManipulator.class);

  private Configuration conf;
  private FileSystem fs;

  public HdfsDataManipulator(Configuration conf) {
    this.conf = conf;
  }

  /**
   * @param path to be deleted.
   * @return true is the path was successfully deleted, or false if not.
   * @throws IOException Occurs if there is a problem getting the file system object which owns the path.
   */
  @Override
  public boolean delete(String path) throws IOException {
    return delete(new Path(path));
  }

  private boolean delete(Path path) throws IOException {
    log.info("Deleting all data at location: {}", path);
    fs = path.getFileSystem(conf);
    return fs.delete(path, true);
  }

}
