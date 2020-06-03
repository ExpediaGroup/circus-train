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
package com.hotels.bdp.circustrain.api.conf;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;

//@Profile({ Modules.REPLICATION })
//@Component
public class DataManipulationClientFactory {

  // factory should create the client
  // should be a spring bean
  // so like how the copiers are passed the stuff they need
  // pass in the stuff this class needs

  private static final String HDFS_LOCATION = "hdfs";
  private static final String S3_LOCATION = "s3";

  // @Autowired
  public DataManipulationClientFactory(Configuration conf, Map<String, Object> copierOptions) {

  }

  public DataManipulationClient newInstance(String path) {

    if (isHdfsPath(path)) {

      return createNewHdfsClient();

    } else if (isS3Path(path)) {
      return createNewS3Client();
    }

    return null;

  }

  private DataManipulationClient createNewHdfsClient() {

    return null;
  }

  private DataManipulationClient createNewS3Client() {

    // new S3S3CopierOptions(copierOptions)
    // targetBase = toAmazonS3URI(replicaLocation.toUri());
    // return new AwsDataManipulationClient(s3ClientFactory.newInstance(targetBase, s3s3CopierOptions));

    return null;
  }

  private boolean isHdfsPath(String path) {
    return path.startsWith(HDFS_LOCATION);
  }

  private boolean isS3Path(String path) {
    return path.startsWith(S3_LOCATION);
  }

}
