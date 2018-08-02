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

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hotels.bdp.circustrain.api.CircusTrainException;

public class GCPCredentialConfigurer {
  private static final Logger LOG = LoggerFactory.getLogger(GCPCredentialConfigurer.class);

  private final Configuration conf;
  private final GCPCredentialPathProvider credentialPathProvider;
  private final DistributedFileSystemPathProvider dfsPathProvider;

  public GCPCredentialConfigurer(
      Configuration conf,
      GCPCredentialPathProvider credentialPathProvider,
      DistributedFileSystemPathProvider dfsPathProvider) {
    this.conf = conf;
    this.credentialPathProvider = credentialPathProvider;
    this.dfsPathProvider = dfsPathProvider;
  }

  public void configureCredentials() {
    try {
      LOG.debug("Configuring GCP Credentials");
      GCPCredentialCopier copier = new GCPCredentialCopier(FileSystem.get(conf), conf, credentialPathProvider,
          dfsPathProvider);
      copier.copyCredentials();
    } catch (IOException e) {
      throw new CircusTrainException(e);
    }
  }
}
