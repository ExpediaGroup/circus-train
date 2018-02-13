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
package com.hotels.bdp.circustrain.aws;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.springframework.stereotype.Component;

import com.hotels.bdp.circustrain.api.CircusTrainException;
import com.hotels.bdp.circustrain.core.util.LibJarDeployer;

@Component
public class BindS3AFileSystem {

  public BindS3AFileSystem() {}

  // Force S3A implementation on source cluster, regardless of what is requested.
  public void bindFileSystem(Configuration configuration) {
    for (String scheme : AWSConstants.S3_FS_PROTOCOLS) {
      configuration.setBoolean(String.format("fs.%s.impl.disable.cache", scheme), true);
      configuration.setClass(String.format("fs.%s.impl", scheme), org.apache.hadoop.fs.s3a.S3AFileSystem.class,
          FileSystem.class);
    }
    loadS3FileSystem(configuration);
  }

  private void loadS3FileSystem(Configuration conf) {
    try {
      new LibJarDeployer().libjars(conf, org.apache.hadoop.fs.s3a.S3AFileSystem.class, BindS3AFileSystem.class);
    } catch (IOException e) {
      throw new CircusTrainException(e);
    }
  }
}
