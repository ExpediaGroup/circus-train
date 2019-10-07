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
package com.hotels.bdp.circustrain.aws;

import java.util.List;
import java.util.Locale;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;

public final class S3Schemes {

  private final static List<String> S3_SCHEMES = ImmutableList.copyOf(AWSConstants.S3_FS_PROTOCOLS);

  private S3Schemes() {}

  /**
   * @param scheme, can be null
   */
  public static boolean isS3Scheme(String scheme) {

    return S3_SCHEMES.contains(Strings.nullToEmpty(scheme).toLowerCase(Locale.ROOT));
  }

}
