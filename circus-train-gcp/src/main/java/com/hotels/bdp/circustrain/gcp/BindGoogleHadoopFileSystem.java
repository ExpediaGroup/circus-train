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
package com.hotels.bdp.circustrain.gcp;

import static com.hotels.bdp.circustrain.gcp.GCPConstants.GCP_PROJECT_ID;
import static com.hotels.bdp.circustrain.gcp.GCPConstants.GCP_SERVICE_ACCOUNT_ENABLE;
import static com.hotels.bdp.circustrain.gcp.GCPConstants.GS_ABSTRACT_FS;
import static com.hotels.bdp.circustrain.gcp.GCPConstants.GS_FS_IMPLEMENTATION;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS;
import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem;

public class BindGoogleHadoopFileSystem {

  private static final Logger LOG = LoggerFactory.getLogger(BindGoogleHadoopFileSystem.class);

  private final Configuration configuration;

  public BindGoogleHadoopFileSystem(Configuration configuration) {
    this.configuration = configuration;
  }

  public void bindFileSystem() {
    LOG.debug("Binding GoogleHadoopFileSystem");
    configuration.set(GS_FS_IMPLEMENTATION, GoogleHadoopFileSystem.class.getName());
    configuration.set(GS_ABSTRACT_FS, GoogleHadoopFS.class.getName());
    configuration.set(GCP_SERVICE_ACCOUNT_ENABLE, "true");
    configuration.set(GCP_PROJECT_ID, "_THIS_VALUE_DOESNT_MATTER");
  }
}
