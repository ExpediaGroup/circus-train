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

import java.nio.file.Paths;

import com.hotels.bdp.circustrain.gcp.context.GCPSecurity;

public class CredentialProviderRelativePathFactory {

  String newInstance(GCPSecurity security) {
    // The DistributedCache of Hadoop can only link files from their relative path to the working directory
    java.nio.file.Path currentDirectory = Paths.get(System.getProperty("user.dir"));
    java.nio.file.Path pathToCredentialsFile = Paths.get(security.getCredentialProvider());
    if (pathToCredentialsFile.isAbsolute()) {
      java.nio.file.Path pathRelative = currentDirectory.relativize(pathToCredentialsFile);
      return pathRelative.toString();
    } else {
      return pathToCredentialsFile.toString();
    }
  }
}
