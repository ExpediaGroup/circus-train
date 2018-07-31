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
package com.hotels.bdp.circustrain.gcp;

import static org.apache.commons.lang.StringUtils.isBlank;

import static com.hotels.bdp.circustrain.gcp.GCPConstants.GCP_KEYFILE_CACHED_LOCATION;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hotels.bdp.circustrain.api.CircusTrainException;
import com.hotels.bdp.circustrain.gcp.context.GCPSecurity;

class GCPCredentialCopier {
  private static final Logger LOG = LoggerFactory.getLogger(GCPCredentialCopier.class);

  private static final String DEFAULT_HDFS_PREFIX = "hdfs:///tmp/ct-gcp-";
  private static final String RANDOM_STRING = UUID.randomUUID().toString() + System.currentTimeMillis();
  private static final String CACHED_CREDENTIAL_NAME = "/ct-gcp-key-" + RANDOM_STRING + ".json";

  private final FileSystem fs;
  private final Configuration conf;
  private final GCPSecurity security;
  private final String credentialProvider;
  private final String hdfsGsCredentialDirectory;
  private final String hdfsGsCredentialAbsolutePath;

  GCPCredentialCopier(FileSystem fs, Configuration conf, GCPSecurity security) {
    this.fs = fs;
    this.conf = conf;
    if (security == null || isBlank(security.getCredentialProvider())) {
      throw new IllegalArgumentException("gcp-security credential-provider must be set");
    }
    this.security = security;
    credentialProvider = security.getCredentialProvider();

    hdfsGsCredentialDirectory = isBlank(security.getDistributedFileSystemWorkingDirectory())
        ? DEFAULT_HDFS_PREFIX + RANDOM_STRING
        : security.getDistributedFileSystemWorkingDirectory();
    hdfsGsCredentialAbsolutePath = hdfsGsCredentialDirectory + CACHED_CREDENTIAL_NAME;
    LOG.info("Temporary HDFS Google Cloud credential location set to {}", hdfsGsCredentialDirectory);
    LOG.info("HDFS Google Cloud credential absolute path will be {}", hdfsGsCredentialAbsolutePath);
  }

  void copyCredentials() {
    try {
      copyCredentialIntoHdfs();
      copyCredentialIntoDistributedCache();
    } catch (IOException | URISyntaxException e) {
      throw new CircusTrainException(e);
    }
  }

  private void copyCredentialIntoHdfs() throws IOException {
    Path source = new Path(credentialProvider);
    Path destination = new Path(hdfsGsCredentialAbsolutePath);
    Path destinationFolder = new Path(hdfsGsCredentialDirectory);
    fs.deleteOnExit(destinationFolder);
    LOG.debug("Copying credential into HDFS {}", destination);
    fs.copyFromLocalFile(source, destination);
  }

  private void copyCredentialIntoDistributedCache() throws URISyntaxException, IOException {
    org.apache.hadoop.mapreduce.filecache.DistributedCache
        .addCacheFile(new URI(hdfsGsCredentialAbsolutePath + "#" + credentialProvider), conf);
    LOG.info("mapreduce.job.cache.files : {}", conf.get("mapreduce.job.cache.files"));
    conf.set(GCP_KEYFILE_CACHED_LOCATION, credentialProvider);
  }
}
