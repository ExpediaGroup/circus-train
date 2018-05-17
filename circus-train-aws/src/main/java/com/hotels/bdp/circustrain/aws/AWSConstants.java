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
package com.hotels.bdp.circustrain.aws;

public final class AWSConstants {

  public static final String ACCESS_KEY = "access.key";
  public static final String SECRET_KEY = "secret.key";

  public static final String FS_PROTOCOL_S3 = "s3";
  public static final String FS_PROTOCOL_S3N = "s3n";
  public static final String FS_PROTOCOL_S3A = "s3a";
  public static final String[] S3_FS_PROTOCOLS = { FS_PROTOCOL_S3, FS_PROTOCOL_S3N, FS_PROTOCOL_S3A };

  private AWSConstants() {}
}
