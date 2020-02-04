/**
 * Copyright (C) 2016-2019 Expedia, Inc.
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
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.hotels.bdp.circustrain.api.CircusTrainException;

@Component
public class GCPCredentialCopier {
  private static final Logger LOG = LoggerFactory.getLogger(GCPCredentialCopier.class);

  public void copyCredentials(
      FileSystem fs,
      Configuration conf,
      GCPCredentialPathProvider credentialPathProvider,
      DistributedFileSystemPathProvider dfsPathProvider) {
    try {
      Path source = credentialPathProvider.newPath();
      Path destination = dfsPathProvider.newPath(conf);
      copyCredentialIntoHdfs(fs, source, destination);
      linkRelativePathInDistributedCache(conf, source, destination);
    } catch (IOException | URISyntaxException e) {
      throw new CircusTrainException(e);
    }
  }

  private void copyCredentialIntoHdfs(FileSystem fs, Path source, Path destination) throws IOException {
    /*
     * The Google credentials file must be present in HDFS so that the DistCP map reduce job can access it upon
     * replication.
     */
    Path destinationFolder = destination.getParent();
    fs.deleteOnExit(destinationFolder);
    LOG.debug("Copying credential into HDFS {}", destination);
    fs.copyFromLocalFile(source, destination);
  }

  private void linkRelativePathInDistributedCache(Configuration conf, Path source, Path destination)
    throws URISyntaxException, IOException {
    /*
     * The "#" links the HDFS location for the key file to the local file system credential provider path so that the
     * GoogleHadoopFileSystem can subsequently resolve it from a local file system uri despite it being in a Distributed
     * file system when the DistCP job runs.
     */
    String cacheFileUri = destination.toString() + "#" + source;
    DistributedCache.addCacheFile(new URI(cacheFileUri), conf);
    LOG.info("mapreduce.job.cache.files : {}", conf.get("mapreduce.job.cache.files"));
    conf.set(GCP_KEYFILE_CACHED_LOCATION, source.toString());
  }
}
