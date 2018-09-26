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

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertNotNull;

import static com.hotels.bdp.circustrain.gcp.GCPConstants.GCP_PROJECT_ID;
import static com.hotels.bdp.circustrain.gcp.GCPConstants.GCP_SERVICE_ACCOUNT_ENABLE;
import static com.hotels.bdp.circustrain.gcp.GCPConstants.GS_ABSTRACT_FS;
import static com.hotels.bdp.circustrain.gcp.GCPConstants.GS_FS_IMPLEMENTATION;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS;
import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem;

public class BindGoogleHadoopFileSystemTest {

  @Test
  public void bindFileSystemTest() throws Exception {
    Configuration conf = new Configuration();
    BindGoogleHadoopFileSystem binder = new BindGoogleHadoopFileSystem();
    binder.bindFileSystem(conf);
    assertNotNull(conf.get(GCP_PROJECT_ID));
    assertEquals("true", conf.get(GCP_SERVICE_ACCOUNT_ENABLE));
    assertEquals(GoogleHadoopFileSystem.class.getName(), conf.get(GS_FS_IMPLEMENTATION));
    assertEquals(GoogleHadoopFS.class.getName(), conf.get(GS_ABSTRACT_FS));
  }
}
