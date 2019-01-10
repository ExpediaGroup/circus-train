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

import java.util.Locale;

import com.amazonaws.services.s3.model.CannedAccessControlList;

public final class CannedAclUtils {

  private CannedAclUtils() {}

  /**
   * Returns a {@link CannedAccessControlList} from its {@code x-amz-acl} value.
   *
   * @param cannedAcl S3 x-amz-acl value
   * @return The corresponding CannedAccessControlList value
   */
  public static CannedAccessControlList toCannedAccessControlList(String cannedAcl) {
    if (cannedAcl == null) {
      return null;
    }

    cannedAcl = cannedAcl.toLowerCase(Locale.ROOT);

    for (CannedAccessControlList acl : CannedAccessControlList.values()) {
      if (acl.toString().equals(cannedAcl)) {
        return acl;
      }
    }

    throw new IllegalArgumentException("CannedAccessControlList does not contain " + cannedAcl);
  }

}
