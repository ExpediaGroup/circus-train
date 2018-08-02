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
package com.hotels.bdp.circustrain.gcp;

import static org.apache.commons.lang3.StringUtils.isBlank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.hotels.bdp.circustrain.gcp.context.GCPSecurity;

@Component
public class DistributedFileSystemPathProvider {

  static final String DEFAULT_HDFS_PREFIX = "hdfs:/tmp/ct-gcp-";
  static final String GCP_KEY_NAME = "ct-gcp-key.json";

  private final GCPSecurity security;
  private final RandomStringFactory randomStringFactory;

  @Autowired
  public DistributedFileSystemPathProvider(GCPSecurity security, RandomStringFactory randomStringFactory) {
    this.security = security;
    this.randomStringFactory = randomStringFactory;
  }

  public Path newPath(Configuration configuration) {
    String randomString = randomStringFactory.newInstance();

    if (isBlank(security.getDistributedFileSystemWorkingDirectory())) {
      Path parentDirectory = getTemporaryFolder(configuration, randomString);
      return new Path(parentDirectory, GCP_KEY_NAME);
    } else {
      Path parentDirectory = new Path(security.getDistributedFileSystemWorkingDirectory(), randomString);
      return new Path(parentDirectory, GCP_KEY_NAME);
    }

  }

  private Path getTemporaryFolder(Configuration configuration, String randomString) {
    String temporaryFolder = configuration.get("hive.exec.scratchdir");
    if (isBlank(temporaryFolder)) {
      return new Path("hdfs:/tmp/ct-gcp-" + randomString);
    }
    return new Path(temporaryFolder, randomString);
  }

}
