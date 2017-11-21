/**
 * Copyright (C) 2016-2017 Expedia Inc.
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
package com.hotels.bdp.circustrain.housekeeping.conf;

import org.hibernate.validator.constraints.NotEmpty;

import com.ulisesbocchio.jasyptspringboot.annotation.EnableEncryptableProperties;

import com.hotels.bdp.circustrain.validation.constraints.URI;

@EnableEncryptableProperties
public class DataSource {
  @NotEmpty(message = "housekeeping.dataSource.driverClassName must not be blank")
  private String driverClassName = "org.h2.Driver";
  @URI(message = "housekeeping.dataSource.url does not seem to be a valid JDBC URI", scheme = "jdbc")
  private String url = null;
  // Housekeeping can create an application internal database if the user doesn't provide DataSource configuration
  // properties and then these are the default credentials used.
  private String username = "bdp";
  private String password = "Ch4ll3ng3";

  public String getDriverClassName() {
    return driverClassName;
  }

  public void setDriverClassName(String driverClassName) {
    this.driverClassName = driverClassName;
  }

  public String getUrl() {
    return url;
  }

  public void setUrl(String url) {
    this.url = url;
  }

  public String getUsername() {
    return username;
  }

  public void setUsername(String username) {
    this.username = username;
  }

  public String getPassword() {
    return password;
  }

  public void setPassword(String password) {
    this.password = password;
  }
}
