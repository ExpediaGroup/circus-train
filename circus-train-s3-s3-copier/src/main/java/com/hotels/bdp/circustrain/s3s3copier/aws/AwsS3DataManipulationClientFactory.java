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

import static com.hotels.bdp.circustrain.aws.AmazonS3URIs.toAmazonS3URI;

import java.net.URI;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import com.amazonaws.services.s3.AmazonS3URI;

import com.hotels.bdp.circustrain.api.Modules;
import com.hotels.bdp.circustrain.api.data.DataManipulationClientFactory;
import com.hotels.bdp.circustrain.aws.AwsDataManipulationClient;
import com.hotels.bdp.circustrain.aws.S3Schemes;
import com.hotels.bdp.circustrain.s3s3copier.S3S3CopierOptions;

@Profile({ Modules.REPLICATION })
@Component
@Order(Ordered.LOWEST_PRECEDENCE - 10)
public class AwsS3DataManipulationClientFactory implements DataManipulationClientFactory {

  private AmazonS3ClientFactory s3ClientFactory;

  @Autowired
  public AwsS3DataManipulationClientFactory(AmazonS3ClientFactory s3ClientFactory) {
    this.s3ClientFactory = s3ClientFactory;
  }

  @Override
  public AwsDataManipulationClient newInstance(Path replicaLocation, Map<String, Object> copierOptions) {
    S3S3CopierOptions s3s3CopierOptions = new S3S3CopierOptions(copierOptions);
    AmazonS3URI replicaLocationUri = toAmazonS3URI(URI.create(replicaLocation.toString()));
    return new AwsDataManipulationClient(s3ClientFactory.newInstance(replicaLocationUri, s3s3CopierOptions));
  }

  /**
   * Checks that the source and replica locations are both S3 locations.
   */
  @Override
  public boolean supportsSchemes(String sourceScheme, String replicaScheme) {
    return S3Schemes.isS3Scheme(sourceScheme) && S3Schemes.isS3Scheme(replicaScheme);
  }


}
