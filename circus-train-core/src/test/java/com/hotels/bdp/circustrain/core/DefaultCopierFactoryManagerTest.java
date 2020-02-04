/**
 * Copyright (C) 2016-2020 Expedia, Inc.
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
package com.hotels.bdp.circustrain.core;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.collect.ImmutableMap;

import com.hotels.bdp.circustrain.api.copier.Copier;
import com.hotels.bdp.circustrain.api.copier.CopierFactory;

@RunWith(MockitoJUnitRunner.class)
public class DefaultCopierFactoryManagerTest {

  private static final String SCHEME = "scheme";

  private final Path path = new Path("scheme://some/path");

  @Mock
  private CopierFactory copierFactory;

  private DefaultCopierFactoryManager defaultCopierFactoryManager;

  @Test
  public void supportsScheme() {
    defaultCopierFactoryManager = new DefaultCopierFactoryManager(Arrays.asList(copierFactory));
    when(copierFactory.supportsSchemes(SCHEME, SCHEME)).thenReturn(true);

    CopierFactory copierFactoryResult = defaultCopierFactoryManager.getCopierFactory(path, path,
        ImmutableMap.<String, Object> of());

    assertEquals(copierFactory, copierFactoryResult);
  }

  @Test(expected = UnsupportedOperationException.class)
  public void doesNotSupportScheme() {
    defaultCopierFactoryManager = new DefaultCopierFactoryManager(Arrays.asList(copierFactory));
    when(copierFactory.supportsSchemes(SCHEME, SCHEME)).thenReturn(false);

    defaultCopierFactoryManager.getCopierFactory(path, path, ImmutableMap.<String, Object> of());
  }

  @Test
  public void supportsSchemeWithCopierFactoryClass() {
    CopierFactory testCopierFactory = new TestCopierFactory();
    defaultCopierFactoryManager = new DefaultCopierFactoryManager(Arrays.asList(testCopierFactory));
    CopierFactory copierFactoryResult = defaultCopierFactoryManager.getCopierFactory(path, path,
        ImmutableMap.<String, Object> of("copier-factory-class", testCopierFactory.getClass().getName()));

    assertEquals(copierFactoryResult, testCopierFactory);
  }

  @Test(expected = UnsupportedOperationException.class)
  public void supportsSchemeWithCopierFactoryClassNotFound() {
    CopierFactory testCopierFactory = new TestCopierFactory();
    defaultCopierFactoryManager = new DefaultCopierFactoryManager(Arrays.asList(testCopierFactory));

    defaultCopierFactoryManager.getCopierFactory(path, path, ImmutableMap.<String, Object> of("copier-factory-class", "test"));
  }

  class TestCopierFactory implements CopierFactory {

    @Override
    public boolean supportsSchemes(String sourceScheme, String replicaScheme) {
      return false;
    }

    @Override
    public Copier newInstance(String eventId, Path sourceBaseLocation, List<Path> sourceSubLocations,
        Path replicaLocation, Map<String, Object> copierOptions) {
      return null;
    }

    @Override
    public Copier newInstance(String eventId, Path sourceBaseLocation, Path replicaLocation,
        Map<String, Object> copierOptions) {
      return null;
    }
  }
}
