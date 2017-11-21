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
package com.hotels.bdp.circustrain.validation.constraintvalidators;

import java.net.URISyntaxException;

import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;

/**
 * Validate that the character sequence (e.g. {@link String}) is a valid URI using the {@code java.net.URI} constructor.
 */
public class URIValidator
    implements ConstraintValidator<com.hotels.bdp.circustrain.validation.constraints.URI, CharSequence> {
  private String scheme;
  private String host;
  private int port;

  @Override
  public void initialize(com.hotels.bdp.circustrain.validation.constraints.URI uri) {
    scheme = uri.scheme();
    host = uri.host();
    port = uri.port();
  }

  @Override
  public boolean isValid(CharSequence value, ConstraintValidatorContext constraintValidatorContext) {
    if (value == null || value.length() == 0) {
      return true;
    }

    java.net.URI uri;
    try {
      uri = new java.net.URI(value.toString());
    } catch (URISyntaxException e) {
      return false;
    }

    if (scheme != null && scheme.length() > 0 && !scheme.equalsIgnoreCase(uri.getScheme())) {
      return false;
    }

    if (host != null && host.length() > 0 && !host.equalsIgnoreCase(uri.getHost())) {
      return false;
    }

    if (port != -1 && uri.getPort() != port) {
      return false;
    }

    return true;
  }
}
