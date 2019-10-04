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
package com.hotels.bdp.circustrain.aws;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

public class AssumeRoleCredentialProviderTest {

  @Test(expected = NullPointerException.class)
  public void getCredentialsThrowsNullPointerException() {
    AssumeRoleCredentialProvider provider = new AssumeRoleCredentialProvider(null);
    provider.getCredentials();
  }

  @Test(expected = NullPointerException.class)
  public void refreshThrowsNullPointerException() {
    AssumeRoleCredentialProvider provider = new AssumeRoleCredentialProvider(null);
    provider.refresh();
    ;
  }

  @Test(expected = IllegalArgumentException.class)
  public void getCredentialsThrowsIllegalArgumentException() {
    AssumeRoleCredentialProvider provider = new AssumeRoleCredentialProvider(new Configuration());
    provider.getCredentials();
  }

  @Test(expected = IllegalArgumentException.class)
  public void refreshThrowsIllegalArgumentException() {
    AssumeRoleCredentialProvider provider = new AssumeRoleCredentialProvider(new Configuration());
    provider.refresh();
    ;
  }
}
