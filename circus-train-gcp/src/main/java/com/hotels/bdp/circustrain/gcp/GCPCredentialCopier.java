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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hotels.bdp.circustrain.api.CircusTrainException;
import com.hotels.bdp.circustrain.gcp.context.GCPSecurity;

class GCPCredentialCopier {
  private static final Logger LOG = LoggerFactory.getLogger(GCPCredentialCopier.class);

  private final FileSystem fs;
  private final Configuration conf;
  private final GCPSecurity security;
  private final CredentialProviderRelativePathFactory credentialProviderRelativePathFactory;
  private final HdfsGsCredentialAbsolutePathFactory hdfsGsCredentialAbsolutePathFactory;
  private final HdfsGsCredentialDirectoryFactory hdfsGsCredentialDirectoryFactory;
  private final RandomStringFactory randomStringFactory;
  private final String credentialsFileRelativePath;
  private final String randomString;
  private final Path hdfsGsCredentialDirectory;
  private final Path hdfsGsCredentialAbsolutePath;

  GCPCredentialCopier(FileSystem fs, Configuration conf, GCPSecurity security) {
    this(fs, conf, security, new CredentialProviderRelativePathFactory(), new HdfsGsCredentialAbsolutePathFactory(),
        new HdfsGsCredentialDirectoryFactory(), new RandomStringFactory());
  }

  GCPCredentialCopier(
      FileSystem fs,
      Configuration conf,
      GCPSecurity security,
      CredentialProviderRelativePathFactory credentialProviderRelativePathFactory,
      HdfsGsCredentialAbsolutePathFactory hdfsGsCredentialAbsolutePathFactory,
      HdfsGsCredentialDirectoryFactory hdfsGsCredentialDirectoryFactory,
      RandomStringFactory randomStringFactory) {
    this.fs = fs;
    this.conf = conf;
    this.credentialProviderRelativePathFactory = credentialProviderRelativePathFactory;
    this.hdfsGsCredentialAbsolutePathFactory = hdfsGsCredentialAbsolutePathFactory;
    this.hdfsGsCredentialDirectoryFactory = hdfsGsCredentialDirectoryFactory;
    this.randomStringFactory = randomStringFactory;

    if (security == null || isBlank(security.getCredentialProvider())) {
      throw new IllegalArgumentException("gcp-security credential-provider must be set");
    }
    this.security = security;

    randomString = this.randomStringFactory.newInstance();
    hdfsGsCredentialDirectory = this.hdfsGsCredentialDirectoryFactory.newInstance(security, randomString);
    hdfsGsCredentialAbsolutePath = this.hdfsGsCredentialAbsolutePathFactory
        .newInstance(hdfsGsCredentialDirectory, randomString);
    credentialsFileRelativePath = this.credentialProviderRelativePathFactory.newInstance(security);
    LOG.debug("Credential Provider URI = {}", credentialsFileRelativePath);
    LOG.debug("Temporary HDFS Google Cloud credential location set to {}", hdfsGsCredentialDirectory.toString());
    LOG.debug("HDFS Google Cloud credential path will be {}", hdfsGsCredentialAbsolutePath.toString());
  }

  void copyCredentials() {
    try {
      copyCredentialIntoHdfs();
      linkRelativePathInDistributedCache();
    } catch (IOException | URISyntaxException e) {
      throw new CircusTrainException(e);
    }
  }

  private void copyCredentialIntoHdfs() throws IOException {
    /*
     * The Google credentials file must be present in HDFS so that the DistCP map reduce job can access it upon
     * replication.
     */
    Path source = new Path(credentialsFileRelativePath);
    Path destination = hdfsGsCredentialAbsolutePath;
    Path destinationFolder = hdfsGsCredentialDirectory;
    fs.deleteOnExit(destinationFolder);
    LOG.debug("Copying credential into HDFS {}", destination);
    fs.copyFromLocalFile(source, destination);
  }

  private void linkRelativePathInDistributedCache() throws URISyntaxException, IOException {
    /*
     * The "#" links the HDFS location for the key file to the local file system credential provider path so that the
     * GoogleHadoopFileSystem can subsequently resolve it from a local file system uri despite it being in a Distributed
     * file system when the DistCP job runs.
     */
    String cacheFileUri = hdfsGsCredentialAbsolutePath.toString() + "#" + credentialsFileRelativePath;
    org.apache.hadoop.mapreduce.filecache.DistributedCache.addCacheFile(new URI(cacheFileUri), conf);

    LOG.info("mapreduce.job.cache.files : {}", conf.get("mapreduce.job.cache.files"));
    conf.set(GCP_KEYFILE_CACHED_LOCATION, credentialsFileRelativePath);
  }
}
