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

import org.apache.hadoop.fs.Path;

import com.hotels.bdp.circustrain.gcp.context.GCPSecurity;

class HdfsGsCredentialDirectoryFactory {

  static final String DEFAULT_HDFS_PREFIX = "hdfs:/tmp/ct-gcp-";
  private final RandomStringFactory randomStringFactory;

  HdfsGsCredentialDirectoryFactory() {
    this(new RandomStringFactory());
  }

  HdfsGsCredentialDirectoryFactory(RandomStringFactory randomStringFactory) {
    this.randomStringFactory = randomStringFactory;
  }

  Path newInstance(GCPSecurity security) {
    String randomString = randomStringFactory.newInstance();
    if (isBlank(security.getDistributedFileSystemWorkingDirectory())) {
      return new Path(DEFAULT_HDFS_PREFIX + randomString);
    } else {
      return new Path(security.getDistributedFileSystemWorkingDirectory(), randomString);
    }

  }

}
