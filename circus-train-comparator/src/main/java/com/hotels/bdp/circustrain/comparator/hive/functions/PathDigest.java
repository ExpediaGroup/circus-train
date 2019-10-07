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
package com.hotels.bdp.circustrain.comparator.hive.functions;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.codec.binary.Base64;

import com.google.common.base.Function;
import com.google.common.base.Strings;

import com.hotels.bdp.circustrain.api.CircusTrainException;
import com.hotels.bdp.circustrain.comparator.hive.wrappers.PathMetadata;

public class PathDigest implements Function<PathMetadata, String> {

  public static final String DEFAULT_MESSAGE_DIGEST_ALGORITHM = "MD5";

  private final MessageDigest messageDigest;

  public PathDigest() {
    this(null);
  }

  public PathDigest(String algorithm) {
    if (Strings.isNullOrEmpty(algorithm)) {
      algorithm = DEFAULT_MESSAGE_DIGEST_ALGORITHM;
    }

    try {
      messageDigest = MessageDigest.getInstance(algorithm);
    } catch (NoSuchAlgorithmException e) {
      throw new CircusTrainException("Unable to find MessageDigest algorithm " + algorithm, e);
    }
  }

  @Override
  public String apply(PathMetadata pathDescriptor) {
    byte[] checksum = messageDigest.digest(serialize(pathDescriptor));
    return Base64.encodeBase64String(checksum);
  }

  public static byte[] serialize(PathMetadata pathDescriptor) {
    try (ByteArrayOutputStream b = new ByteArrayOutputStream()) {
      try (ObjectOutputStream o = new ObjectOutputStream(new GZIPOutputStream(b))) {
        o.writeObject(pathDescriptor);
      }
      return b.toByteArray();
    } catch (IOException e) {
      throw new CircusTrainException("Unable to serialize pathDescriptor " + pathDescriptor, e);
    }
  }
}
