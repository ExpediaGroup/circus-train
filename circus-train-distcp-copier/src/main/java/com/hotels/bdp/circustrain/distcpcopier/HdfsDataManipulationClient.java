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

import com.hotels.bdp.circustrain.api.conf.DataManipulationClient;

public class HdfsDataManipulationClient implements DataManipulationClient {

  private static final Logger LOG = LoggerFactory.getLogger(HdfsDataManipulationClient.class);

  private Configuration conf;

  public HdfsDataManipulationClient(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public void delete(String path) throws IOException {
    delete(new Path(path));
  }

  public void delete(Path path) throws IOException {
    try {
    FileSystem fs = path.getFileSystem(conf);
    fs.delete(path, true);
    } catch (IOException e) {
      LOG.info("Unable to delete path:{}", path);
    }
  }

}
