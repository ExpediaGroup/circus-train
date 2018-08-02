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

class GCPCredentialCopier {
  private static final Logger LOG = LoggerFactory.getLogger(GCPCredentialCopier.class);
  static final String GCP_KEY_NAME = "ct-gcp-key.json";

  private final FileSystem fs;
  private final Configuration conf;
  private final Path credentialsFileRelativePath;
  private final Path dfsGCPCredentialDirectory;
  private final Path dfsGCPCredentialAbsolutePath;

  GCPCredentialCopier(FileSystem fs, Configuration conf, GCPCredentialPathProvider credentialPathProvider, DistributedFileSystemPathProvider dfsPathProvider) {
    this.fs = fs;
    this.conf = conf;

    dfsGCPCredentialDirectory = dfsPathProvider.newPath();
    dfsGCPCredentialAbsolutePath = new Path(dfsGCPCredentialDirectory, GCP_KEY_NAME);
    credentialsFileRelativePath = credentialPathProvider.newPath();
    LOG.debug("Credential Provider URI = {}", credentialsFileRelativePath);
    LOG.debug("Temporary HDFS Google Cloud credential location set to {}", dfsGCPCredentialDirectory.toString());
    LOG.debug("HDFS Google Cloud credential path will be {}", dfsGCPCredentialAbsolutePath.toString());

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
    Path source = credentialsFileRelativePath;
    Path destination = dfsGCPCredentialAbsolutePath;
    Path destinationFolder = dfsGCPCredentialDirectory;
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
    String cacheFileUri = dfsGCPCredentialAbsolutePath.toString() + "#" + credentialsFileRelativePath;
    org.apache.hadoop.mapreduce.filecache.DistributedCache.addCacheFile(new URI(cacheFileUri), conf);

    LOG.info("mapreduce.job.cache.files : {}", conf.get("mapreduce.job.cache.files"));
    conf.set(GCP_KEYFILE_CACHED_LOCATION, credentialsFileRelativePath.toString());
  }
}
