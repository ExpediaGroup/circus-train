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

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.core.env.ConfigurableEnvironment;

@RunWith(MockitoJUnitRunner.class)
public class ExtensionInitializerTest {
  @Mock
  private AnnotationConfigApplicationContext context;
  @Mock
  private ConfigurableEnvironment env;
  @Mock
  private ExtensionPackageProvider provider;

  private ExtensionInitializer underTest;

  @Before
  public void before() {
    when(context.getEnvironment()).thenReturn(env);
    underTest = new ExtensionInitializer(provider);
  }

  @Test
  public void noPackage() {
    when(provider.getPackageNames(env)).thenReturn(Collections.<String> emptySet());

    underTest.initialize(context);

    verify(context, never()).scan(any(String[].class));
  }

  @Test
  public void singlePackages() {
    when(provider.getPackageNames(env)).thenReturn(Collections.singleton("com.foo.bar"));

    underTest.initialize(context);

    verify(context).scan(new String[] { "com.foo.bar" });
  }
}
