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

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.fs.Path;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import com.hotels.bdp.circustrain.gcp.context.GCPSecurity;

@RunWith(MockitoJUnitRunner.class)
public class GCPCredentialPathProviderTest {

  private final GCPSecurity security = new GCPSecurity();

  @Test
  public void newInstanceWithRelativePath() {
    String relativePath = "../test.json";
    security.setCredentialProvider(relativePath);
    String result = new GCPCredentialPathProvider(security).newPath().toString();
    assertThat(result, is(relativePath));
  }

  @Test
  public void newInstanceWithAbsolutePath() {
    security.setCredentialProvider("/test.json");
    String result = new GCPCredentialPathProvider(security).newPath().toString();
    assertFalse(new Path(result).isAbsolute());
    assertTrue(result.startsWith("../"));
  }

  @Test
  public void newInstanceWithBlankPath() {
    security.setCredentialProvider("");
    Path result = new GCPCredentialPathProvider(security).newPath();
    assertNull(result);

  }

}
