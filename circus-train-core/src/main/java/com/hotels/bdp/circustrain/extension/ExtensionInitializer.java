/**
 * Copyright (C) 2016-2019 Expedia, Inc.
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

import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import com.google.common.annotations.VisibleForTesting;

public class ExtensionInitializer implements ApplicationContextInitializer<AnnotationConfigApplicationContext> {
  private static final Logger LOG = LoggerFactory.getLogger(ExtensionInitializer.class);

  private final ExtensionPackageProvider provider;

  public ExtensionInitializer() {
    this(new PropertyExtensionPackageProvider());
  }

  @VisibleForTesting
  ExtensionInitializer(ExtensionPackageProvider provider) {
    this.provider = provider;
  }

  @Override
  public void initialize(AnnotationConfigApplicationContext context) {
    Set<String> packageNames = provider.getPackageNames(context.getEnvironment());
    if (packageNames.size() > 0) {
      LOG.info("Adding packageNames '{}' to component scan.", packageNames);
      context.scan(packageNames.toArray(new String[packageNames.size()]));
    }
  }
}
