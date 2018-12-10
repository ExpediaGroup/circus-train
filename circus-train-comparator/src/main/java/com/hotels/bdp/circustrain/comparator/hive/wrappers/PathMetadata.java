/**
 * Copyright (C) 2016-2018 Expedia Inc.
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
package com.hotels.bdp.circustrain.comparator.hive.wrappers;

import java.io.Serializable;
import java.util.List;

import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.Path;

import com.google.common.collect.ImmutableList;

public class PathMetadata implements Serializable {

  private static final long serialVersionUID = 1L;

  private final String location;
  private final long lastModifiedTimestamp;
  private final String checkSumAlgorithmName;
  private final int checkSumLength;
  private final byte[] checksum;
  private final List<PathMetadata> childrenMetadata;

  public PathMetadata(
      Path location,
      long lastModifiedTimestamp,
      FileChecksum checksum,
      List<PathMetadata> childrenMetadata) {
    this.location = location.toUri().toString();
    this.lastModifiedTimestamp = lastModifiedTimestamp;
    if (checksum == null) {
      checkSumAlgorithmName = null;
      checkSumLength = 0;
      this.checksum = null;
    } else {
      checkSumAlgorithmName = checksum.getAlgorithmName();
      checkSumLength = checksum.getLength();
      this.checksum = checksum.getBytes();
    }
    this.childrenMetadata = childrenMetadata == null ? ImmutableList.<PathMetadata>of()
        : ImmutableList.copyOf(childrenMetadata);
  }

  public String getLocation() {
    return location;
  }

  public long getLastModifiedTimestamp() {
    return lastModifiedTimestamp;
  }

  public String getChecksumAlgorithmName() {
    return checkSumAlgorithmName;
  }

  public int getChecksumLength() {
    return checkSumLength;
  }

  public byte[] getChecksum() {
    return checksum;
  }

  public List<PathMetadata> getChildrenMetadata() {
    return childrenMetadata;
  }

}
