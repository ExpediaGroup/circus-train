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
package com.hotels.bdp.circustrain.core.metastore;

import com.hotels.bdp.circustrain.core.conf.MetastoreTunnel;

public enum CircusTrainHiveConfVars {

  SSH_LOCALHOST("com.hotels.bdp.circustrain.core.metastore.client.ssh.localhost", MetastoreTunnel.DEFAULT_LOCALHOST),
  SSH_PORT("com.hotels.bdp.circustrain.core.metastore.client.ssh.port", MetastoreTunnel.DEFAULT_PORT),
  SSH_ROUTE("com.hotels.bdp.circustrain.core.metastore.client.ssh.route", null),
  SSH_PRIVATE_KEYS("com.hotels.bdp.circustrain.core.metastore.client.ssh.private_keys", null),
  SSH_KNOWN_HOSTS("com.hotels.bdp.circustrain.core.metastore.client.ssh.known_hosts", null),
  SSH_SESSION_TIMEOUT("com.hotels.bdp.circustrain.core.metastore.client.ssh.session_timeout",
      MetastoreTunnel.DEFAULT_SESSION_TIMEOUT),
  SSH_STRICT_HOST_KEY_CHECKING("com.hotels.bdp.circustrain.core.metastore.client.ssh.strict_host_key_checking",
      MetastoreTunnel.DEFAULT_STRICT_HOST_KEY_CHECK);

  public final String varname;
  public final String defaultValue;

  private CircusTrainHiveConfVars(String varname, int defaultValue) {
    this(varname, String.valueOf(defaultValue));
  }

  private CircusTrainHiveConfVars(String varname, String defaultValue) {
    this.varname = varname;
    this.defaultValue = defaultValue;
  }

}
