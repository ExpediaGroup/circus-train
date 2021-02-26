/**
 * Copyright (C) 2016-2021 Expedia, Inc.
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
package com.hotels.bdp.circustrain.extension;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.when;

import java.util.Iterator;
import java.util.Set;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.core.env.ConfigurableEnvironment;

import com.google.common.collect.ImmutableSet;

@RunWith(MockitoJUnitRunner.class)
public class PropertyExtensionPackageProviderTest {
  @Mock
  private ConfigurableEnvironment env;

  private final PropertyExtensionPackageProvider underTest = new PropertyExtensionPackageProvider();

  @Test
  public void noPackagesDeclared() {
    when(env.getProperty("extension-packages", Set.class)).thenReturn(ImmutableSet.of());

    Set<String> packages = underTest.getPackageNames(env);

    assertThat(packages.size(), is(0));
  }

  @Test
  public void twoPackagesDeclared() {
    when(env.getProperty("extension-packages", Set.class)).thenReturn(ImmutableSet.of("com.foo", "com.bar"));

    Set<String> packages = underTest.getPackageNames(env);

    assertThat(packages.size(), is(2));
    Iterator<String> iterator = packages.iterator();
    assertThat(iterator.next(), is("com.foo"));
    assertThat(iterator.next(), is("com.bar"));
  }
}
