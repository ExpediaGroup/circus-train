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
package com.hotels.bdp.circustrain.s3mapreducecpcopier;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.security.alias.CredentialProviderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.MetricRegistry;

import com.hotels.bdp.circustrain.api.CircusTrainException;
import com.hotels.bdp.circustrain.api.conf.DataManipulationClient;
import com.hotels.bdp.circustrain.api.copier.Copier;
import com.hotels.bdp.circustrain.api.copier.CopierOptions;
import com.hotels.bdp.circustrain.api.metrics.Metrics;
import com.hotels.bdp.circustrain.aws.AwsDataManipulationClient;
import com.hotels.bdp.circustrain.metrics.JobCounterGauge;
import com.hotels.bdp.circustrain.metrics.JobMetrics;
import com.hotels.bdp.circustrain.s3mapreducecp.S3MapReduceCp;
import com.hotels.bdp.circustrain.s3mapreducecp.S3MapReduceCpOptions;
import com.hotels.bdp.circustrain.s3mapreducecp.SimpleCopyListing;
import com.hotels.bdp.circustrain.s3mapreducecp.aws.AwsS3ClientFactory;
import com.hotels.bdp.circustrain.s3mapreducecp.mapreduce.Counter;

public class S3MapReduceCpCopier implements Copier {

  interface S3MapReduceCpExecutor {

    static final S3MapReduceCpExecutor DEFAULT = new S3MapReduceCpExecutor() {

      private S3MapReduceCp s3MapReduceCp;
      @Override
      public Job exec(Configuration conf, S3MapReduceCpOptions options) throws Exception {
        this.s3MapReduceCp = new S3MapReduceCp(conf, options);
        return s3MapReduceCp.execute();
      }

      public S3MapReduceCp getS3MapReduceCp() {
        return s3MapReduceCp;
      }
    };

    Job exec(Configuration conf, S3MapReduceCpOptions options) throws Exception;

    S3MapReduceCp getS3MapReduceCp(); // ?????????????????

  }

  private static final Logger LOG = LoggerFactory.getLogger(S3MapReduceCpCopier.class);

  private final Configuration conf;
  private final Path sourceDataBaseLocation;
  private final List<Path> sourceDataLocations;
  private final Path replicaDataLocation;
  private final Map<String, Object> copierOptions;
  private final S3MapReduceCpExecutor executor;

  private final MetricRegistry registry;

  public S3MapReduceCpCopier(
      Configuration conf,
      Path sourceDataBaseLocation,
      List<Path> sourceDataLocations,
      Path replicaDataLocation,
      Map<String, Object> copierOptions,
      MetricRegistry registry) {
    this(conf, sourceDataBaseLocation, sourceDataLocations, replicaDataLocation, copierOptions,
        S3MapReduceCpExecutor.DEFAULT, registry);
  }

  S3MapReduceCpCopier(
      Configuration conf,
      Path sourceDataBaseLocation,
      List<Path> sourceDataLocations,
      Path replicaDataLocation,
      Map<String, Object> copierOptions,
      S3MapReduceCpExecutor executor,
      MetricRegistry registry) {
    this.executor = executor;
    this.registry = registry;
    this.conf = new Configuration(conf); // a copy as we'll be modifying it
    this.sourceDataBaseLocation = sourceDataBaseLocation;
    this.sourceDataLocations = sourceDataLocations;
    this.replicaDataLocation = replicaDataLocation;
    this.copierOptions = copierOptions;
  }

  private S3MapReduceCpOptions parseCopierOptions(Map<String, Object> copierOptions) {
    String defaultCredentialsProviderString = conf.get(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH);
    URI defaultCredentialsProvider = null;
    if (defaultCredentialsProviderString != null) {
      defaultCredentialsProvider = URI.create(defaultCredentialsProviderString);
    }

    URI replicaDataLocationUri = toURI(replicaDataLocation);
    S3MapReduceCpOptionsParser optionsParser = null;
    if (sourceDataLocations.isEmpty()) {
      LOG.debug("Will copy all sub-paths.");
      optionsParser = new S3MapReduceCpOptionsParser(Arrays.asList(sourceDataBaseLocation), replicaDataLocationUri,
          defaultCredentialsProvider);
    } else {
      LOG.debug("Will copy {} sub-paths.", sourceDataLocations.size());
      conf.set(SimpleCopyListing.CONF_LABEL_ROOT_PATH, sourceDataBaseLocation.toUri().toString());
      optionsParser = new S3MapReduceCpOptionsParser(sourceDataLocations, replicaDataLocationUri,
          defaultCredentialsProvider);
    }
    return optionsParser.parse(copierOptions);
  }

  private URI toURI(Path replicaDataLocation) {
    if (Boolean.parseBoolean((String) copierOptions.get(CopierOptions.COPY_DESTINATION_IS_FILE))) {
      return replicaDataLocation.toUri();
    } else {
      return toDirectoryUri(replicaDataLocation);
    }
  }

  private URI toDirectoryUri(Path path) {
    String uri = path.toUri().toString();
    if (!uri.endsWith("/")) {
      uri += "/";
    }
    return URI.create(uri);
  }

  @Override
  public Metrics copy() throws CircusTrainException {
    LOG.info("Copying table data.");
    LOG.debug("Invoking S3MapReduceCp: {} -> {}", sourceDataBaseLocation, replicaDataLocation);

    S3MapReduceCpOptions s3MapReduceCpOptions = parseCopierOptions(copierOptions);
    LOG.debug("Invoking S3MapReduceCp with options: {}", s3MapReduceCpOptions);

    try {
      Enum<?> counter = Counter.BYTESCOPIED;
      Job job = executor.exec(conf, s3MapReduceCpOptions);
      registerRunningJobMetrics(job, counter);
      if (!job.waitForCompletion(true)) {
        throw new IOException(
            "S3MapReduceCp failure: Job " + job.getJobID() + " has failed: " + job.getStatus().getFailureInfo());
      }

      return new JobMetrics(job, counter);
    } catch (Exception e) {
      cleanUpReplicaDataLocation();
      throw new CircusTrainException("Unable to copy file(s)", e);
    }
  }

  private void registerRunningJobMetrics(final Job job, final Enum<?> counter) {
    registry.remove(RunningMetrics.S3_MAPREDUCE_CP_BYTES_REPLICATED.name());
    registry.register(RunningMetrics.S3_MAPREDUCE_CP_BYTES_REPLICATED.name(), new JobCounterGauge(job, counter));

  }

  private void cleanUpReplicaDataLocation() {
    try {
      FileSystem fs = replicaDataLocation.getFileSystem(conf);
      fs.delete(replicaDataLocation, true);
    } catch (Exception e) {
      LOG.error("Unable to clean up replica data location {} after DistCp failure", replicaDataLocation.toUri(), e);
    }
  }

  @Override
  public DataManipulationClient getClient() {
    return new AwsDataManipulationClient(new AwsS3ClientFactory().newInstance(conf));
  }

}
