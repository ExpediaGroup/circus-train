/**
 * Copyright (C) 2016-2019 Expedia Inc.
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

class GCPConstants {
  static final String GCP_PROJECT_ID = "fs.gs.project.id";
  static final String GCP_SERVICE_ACCOUNT_ENABLE = "google.cloud.auth.service.account.enable";
  static final String GCP_KEYFILE_CACHED_LOCATION = "google.cloud.auth.service.account.json.keyfile";
  static final String GS_FS_IMPLEMENTATION = "fs.gs.impl";
  static final String GS_ABSTRACT_FS = "fs.AbstractFileSystem.gs.impl";

  private GCPConstants() {}
}
