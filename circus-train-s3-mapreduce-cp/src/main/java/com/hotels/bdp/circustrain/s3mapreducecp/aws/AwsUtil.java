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
package com.hotels.bdp.circustrain.s3mapreducecp.aws;

import java.util.Locale;

import com.amazonaws.services.s3.model.StorageClass;

public final class AwsUtil {

  private AwsUtil() {}

  public static StorageClass toStorageClass(String storageClass) {
    if (storageClass == null) {
      return null;
    }
    return StorageClass.fromValue(storageClass.toUpperCase(Locale.ROOT));
  }

}
