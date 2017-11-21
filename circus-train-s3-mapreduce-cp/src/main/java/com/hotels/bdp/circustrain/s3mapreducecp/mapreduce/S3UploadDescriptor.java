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
package com.hotels.bdp.circustrain.s3mapreducecp.mapreduce;

import org.apache.hadoop.fs.Path;

import com.amazonaws.services.s3.model.ObjectMetadata;

class S3UploadDescriptor {

  private final Path source;
  private final String bucketName;
  private final String key;
  private final ObjectMetadata metadata;
  private Path targetPath;

  S3UploadDescriptor(Path source, String bucketName, String key, ObjectMetadata metadata) {
    this.source = source;
    this.bucketName = bucketName;
    this.key = key;
    this.metadata = metadata;
  }

  public Path getSource() {
    return source;
  }

  public String getBucketName() {
    return bucketName;
  }

  public String getKey() {
    return key;
  }

  public ObjectMetadata getMetadata() {
    return metadata;
  }

  public Path getTargetPath() {
    if (targetPath == null) {
      targetPath = new Path(new StringBuilder("s3://").append(bucketName).append("/").append(key).toString());
    }
    return targetPath;
  }

}
