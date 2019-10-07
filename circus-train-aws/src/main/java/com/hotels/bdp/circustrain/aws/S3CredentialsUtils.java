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
package com.hotels.bdp.circustrain.aws;

import static com.hotels.bdp.circustrain.aws.AWSConstants.ACCESS_KEY;
import static com.hotels.bdp.circustrain.aws.AWSConstants.SECRET_KEY;
import static com.hotels.bdp.circustrain.aws.AWSCredentialUtils.getKey;

import org.apache.hadoop.conf.Configuration;
import org.springframework.stereotype.Component;

@Component
public class S3CredentialsUtils {

  /**
   * Adds the {@code s3}, {@code s3n} & {@code s3a} Credentials to the {@link Configuration} using the Hadoop Credential
   * Provider. It expects {@code access.key} and {@code secret.key} to be available secured passwords.
   *
   * @param conf Hadoop Configuration
   */
  public void setS3Credentials(Configuration conf) {
    String accessKey = getKey(conf, ACCESS_KEY);
    String secretKey = getKey(conf, SECRET_KEY);

    conf.set("fs.s3.awsAccessKeyId", accessKey);
    conf.set("fs.s3.awsSecretAccessKey", secretKey);

    conf.set("fs.s3n.awsAccessKeyId", accessKey);
    conf.set("fs.s3n.awsSecretAccessKey", secretKey);

    conf.set("fs.s3a.access.key", accessKey);
    conf.set("fs.s3a.secret.key", secretKey);
  }

}
