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
package com.hotels.bdp.circustrain.aws;

import static java.lang.String.format;

import static org.apache.hadoop.security.alias.CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

public final class AWSCredentialUtils {

  private AWSCredentialUtils() {}

  public static Configuration configureCredentialProvider(String credentialProviderPath) {
    return configureCredentialProvider(credentialProviderPath, new Configuration());
  }

  public static Configuration configureCredentialProvider(String credentialProviderPath, Configuration conf) {
    checkNotNull(credentialProviderPath, "credentialProviderPath cannot be null");
    checkNotNull(conf, "conf cannot be null");
    conf.set(CREDENTIAL_PROVIDER_PATH, credentialProviderPath);
    return conf;
  }

  static String getKey(Configuration conf, String keyType) {
    checkNotNull(conf, "conf cannot be null");
    checkNotNull(keyType, "KeyType cannot be null");

    try {
      char[] key = conf.getPassword(keyType);
      if (key == null) {
        throw new IllegalStateException(format("Unable to get value of '%s'", keyType));
      }
      return new String(key);
    } catch (IOException e) {
      throw new RuntimeException(format("Error getting key for '%s' from credential provider", keyType), e);
    }
  }

}
