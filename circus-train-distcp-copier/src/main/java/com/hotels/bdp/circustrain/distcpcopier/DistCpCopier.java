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
package com.hotels.bdp.circustrain.distcpcopier;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.hotels.bdp.circustrain.api.CircusTrainException;
import com.hotels.bdp.circustrain.api.copier.Copier;
import com.hotels.bdp.circustrain.api.metrics.Metrics;
import com.hotels.bdp.circustrain.core.util.LibJarDeployer;
import com.hotels.bdp.circustrain.metrics.JobMetrics;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.FileSystemCounter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.tools.DistCp;
import org.apache.hadoop.tools.DistCpOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static java.util.Collections.singletonList;

public class DistCpCopier implements Copier {

  interface DistCpExecutor {

    static final DistCpExecutor DEFAULT = new DistCpExecutor() {
      @Override
      public Job exec(Configuration conf, DistCpOptions options) throws Exception {
        return new DistCp(conf, options).execute();
      }
    };

    Job exec(Configuration conf, DistCpOptions options) throws Exception;
  }

  private static final Logger LOG = LoggerFactory.getLogger(DistCpCopier.class);

  private final Configuration conf;
  private final Path sourceDataBaseLocation;
  private final List<Path> sourceDataLocations;
  private final Path replicaDataLocation;
  private final Map<String, Object> copierOptions;
  private final DistCpExecutor executor;

  private final MetricRegistry registry;

  public DistCpCopier(
      Configuration conf,
      Path sourceDataBaseLocation,
      List<Path> sourceDataLocations,
      Path replicaDataLocation,
      Map<String, Object> copierOptions,
      MetricRegistry registry) {
    this(conf, sourceDataBaseLocation, sourceDataLocations, replicaDataLocation, copierOptions, DistCpExecutor.DEFAULT,
        registry);
  }

  DistCpCopier(
      Configuration conf,
      Path sourceDataBaseLocation,
      List<Path> sourceDataLocations,
      Path replicaDataLocation,
      Map<String, Object> copierOptions,
      DistCpExecutor executor,
      MetricRegistry registry) {
    this.executor = executor;
    this.registry = registry;
    this.conf = new Configuration(conf); // a copy as we'll be modifying it
    this.sourceDataBaseLocation = sourceDataBaseLocation;
    this.sourceDataLocations = sourceDataLocations;
    this.replicaDataLocation = replicaDataLocation;
    this.copierOptions = copierOptions;
  }

  private DistCpOptions parseCopierOptions(Map<String, Object> copierOptions) {
    DistCpOptionsParser distCpOptionsParser;
    if (sourceDataLocations.isEmpty()) {
      LOG.debug("Will copy all sub-paths.");
      distCpOptionsParser = new DistCpOptionsParser(singletonList(sourceDataBaseLocation), replicaDataLocation);
    } else {
      LOG.debug("Will copy {} sub-paths.", sourceDataLocations.size());
      distCpOptionsParser = new DistCpOptionsParser(sourceDataLocations, replicaDataLocation);
    }
    return distCpOptionsParser.parse(copierOptions);
  }

  @Override
  public Metrics copy() throws CircusTrainException {
    LOG.info("Copying table data.");
    LOG.debug("Invoking DistCp: {} -> {}", sourceDataBaseLocation, replicaDataLocation);

    DistCpOptions distCpOptions = parseCopierOptions(copierOptions);
    LOG.debug("Invoking DistCp with options: {}", distCpOptions);

    CircusTrainCopyListing.setAsCopyListingClass(conf);
    CircusTrainCopyListing.setRootPath(conf, sourceDataBaseLocation);

    try {
      loadDistributedFileSystems();
      distCpOptions.setBlocking(false);
      Job job = executor.exec(conf, distCpOptions);
      String counter = String.format("%s_BYTES_WRITTEN", replicaDataLocation.toUri().getScheme().toUpperCase());
      registerRunningJobMetrics(job, counter);
      if (!job.waitForCompletion(true)) {
        throw new IOException(
            "DistCp failure: Job " + job.getJobID() + " has failed: " + job.getStatus().getFailureInfo());
      }

      return new JobMetrics(job, FileSystemCounter.class.getName(), counter);
    } catch (Exception e) {
      cleanUpReplicaDataLocation();
      throw new CircusTrainException("Unable to copy file(s)", e);
    }
  }

  private void registerRunningJobMetrics(final Job job, final String counter) {
    registry.remove(RunningMetrics.DIST_CP_BYTES_REPLICATED.name());
    registry.register(RunningMetrics.DIST_CP_BYTES_REPLICATED.name(), new Gauge<Long>() {
      @Override
      public Long getValue() {
        try {
          return job.getCounters().findCounter(FileSystemCounter.class.getName(), counter).getValue();
        } catch (IOException e) {
          LOG.warn("Could not get value for counter " + counter, e);
        }
        return 0L;
      }
    });
  }

  private void loadDistributedFileSystems() throws IOException {
    new LibJarDeployer().libjars(conf, org.apache.hadoop.fs.s3a.S3AFileSystem.class,
        com.hotels.bdp.circustrain.aws.BindS3AFileSystem.class,
        com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem.class,
        com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS.class);
  }

  private void cleanUpReplicaDataLocation() {
    try {
      FileSystem fs = replicaDataLocation.getFileSystem(conf);
      fs.delete(replicaDataLocation, true);
    } catch (Exception e) {
      LOG.error("Unable to clean up replica data location {} after DistCp failure", replicaDataLocation.toUri(), e);
    }
  }
}
