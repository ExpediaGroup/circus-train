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
package com.hotels.bdp.circustrain.s3s3copier;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import com.codahale.metrics.MetricRegistry;

import com.hotels.bdp.circustrain.api.Modules;
import com.hotels.bdp.circustrain.api.copier.Copier;
import com.hotels.bdp.circustrain.api.copier.CopierFactory;
import com.hotels.bdp.circustrain.aws.S3Schemes;
import com.hotels.bdp.circustrain.s3s3copier.aws.AmazonS3ClientFactory;
import com.hotels.bdp.circustrain.s3s3copier.aws.ListObjectsRequestFactory;
import com.hotels.bdp.circustrain.s3s3copier.aws.TransferManagerFactory;

@Profile({ Modules.REPLICATION })
@Component
@Order(Ordered.LOWEST_PRECEDENCE - 10)
public class S3S3CopierFactory implements CopierFactory {

  private final AmazonS3ClientFactory clientFactory;
  private final ListObjectsRequestFactory listObjectsRequestFactory;
  private final TransferManagerFactory transferManagerFactory;
  private final MetricRegistry runningMetricsRegistry;

  @Autowired
  public S3S3CopierFactory(
      AmazonS3ClientFactory clientFactory,
      ListObjectsRequestFactory listObjectsRequestFactory,
      TransferManagerFactory transferManagerFactory,
      MetricRegistry runningMetricsRegistry) {
    this.clientFactory = clientFactory;
    this.listObjectsRequestFactory = listObjectsRequestFactory;
    this.transferManagerFactory = transferManagerFactory;
    this.runningMetricsRegistry = runningMetricsRegistry;
  }

  @Override
  public boolean supportsSchemes(String sourceScheme, String replicaScheme) {
    return S3Schemes.isS3Scheme(sourceScheme) && S3Schemes.isS3Scheme(replicaScheme);
  }

  @Override
  public Copier newInstance(
      String eventId,
      Path sourceBaseLocation,
      List<Path> sourceSubLocations,
      Path replicaLocation,
      Map<String, Object> copierOptions) {
    return new S3S3Copier(sourceBaseLocation, sourceSubLocations, replicaLocation, clientFactory,
        transferManagerFactory, listObjectsRequestFactory, runningMetricsRegistry,
        new S3S3CopierOptions(copierOptions));
  }

  @Override
  public Copier newInstance(
      String eventId,
      Path sourceBaseLocation,
      Path replicaLocation,
      Map<String, Object> copierOptions) {
    return newInstance(eventId, sourceBaseLocation, Collections.<Path>emptyList(), replicaLocation, copierOptions);
  }

}
