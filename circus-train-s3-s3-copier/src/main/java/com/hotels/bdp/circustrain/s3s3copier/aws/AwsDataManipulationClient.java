/**
 * Copyright (C) 2016-2020 Expedia, Inc.
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
package com.hotels.bdp.circustrain.s3s3copier.aws;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.services.s3.model.DeleteObjectRequest;

import com.hotels.bdp.circustrain.api.conf.DataManipulationClient;

public class AwsDataManipulationClient implements DataManipulationClient {

  private AmazonS3 s3Client;

  public AwsDataManipulationClient(AmazonS3 s3Client) {
    this.s3Client = s3Client;
  }

  @Override
  public void delete(String path) {
    System.out.println(">>> S3 client Dropping data at : " + path);
    AmazonS3URI uri = new AmazonS3URI(path);

    // s3Client.deleteObject(uri.getBucket(), uri.getKey());

    DeleteObjectRequest request = new DeleteObjectRequest(uri.getBucket(), uri.getKey());
    s3Client.deleteObject(request);
  }

  // create a new client here?
  // public void newInstance(AmazonS3URI targetBase, S3S3CopierOptions s3s3CopierOptions) {
  //
  //
  // AmazonS3 targetClient = s3ClientFactory.newInstance(targetBase, s3s3CopierOptions);
  //
  // }

}
