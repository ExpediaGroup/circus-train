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
package com.hotels.bdp.circustrain.api.event;

public class EventMetastoreTunnel {

  private final String route;
  private final int port;
  private final String localhost;
  private final String privateKey;
  private final String knownHosts;

  public EventMetastoreTunnel(String route, int port, String localhost, String privateKey, String knownHosts) {
    this.route = route;
    this.port = port;
    this.localhost = localhost;
    this.privateKey = privateKey;
    this.knownHosts = knownHosts;
  }

  public String getRoute() {
    return route;
  }

  public int getPort() {
    return port;
  }

  public String getLocalhost() {
    return localhost;
  }

  public String getPrivateKey() {
    return privateKey;
  }

  public String getKnownHosts() {
    return knownHosts;
  }

}
