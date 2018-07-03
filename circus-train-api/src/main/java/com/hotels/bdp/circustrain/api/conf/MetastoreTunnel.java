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
package com.hotels.bdp.circustrain.api.conf;

import static javax.validation.constraints.Pattern.Flag.CASE_INSENSITIVE;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.Pattern;

import org.hibernate.validator.constraints.NotBlank;

import com.hotels.bdp.circustrain.api.validation.constraints.TunnelRoute;

public class MetastoreTunnel {

  public static final int DEFAULT_PORT = 22;
  public static final String DEFAULT_LOCALHOST = "localhost";
  public static final int DEFAULT_SESSION_TIMEOUT = 60000;
  public static final String DEFAULT_STRICT_HOST_KEY_CHECK = "yes";

  private @NotBlank @TunnelRoute String route;
  private @Min(1) @Max(65535) int port = DEFAULT_PORT;
  private String localhost = DEFAULT_LOCALHOST;
  private @NotBlank String privateKeys;
  private @NotBlank String knownHosts;
  private @Min(0) int timeout = DEFAULT_SESSION_TIMEOUT;

  @NotBlank
  @Pattern(regexp = "yes|no", flags = { CASE_INSENSITIVE })
  private String strictHostKeyChecking = DEFAULT_STRICT_HOST_KEY_CHECK;

  public String getRoute() {
    return route;
  }

  public void setRoute(String route) {
    this.route = route;
  }

  public int getPort() {
    return port;
  }

  public void setPort(int port) {
    this.port = port;
  }

  public String getLocalhost() {
    return localhost;
  }

  public void setLocalhost(String localhost) {
    this.localhost = localhost;
  }

  public String getPrivateKeys() {
    return privateKeys;
  }

  public void setPrivateKeys(String privateKeys) {
    this.privateKeys = privateKeys;
  }

  public String getKnownHosts() {
    return knownHosts;
  }

  public void setKnownHosts(String knownHosts) {
    this.knownHosts = knownHosts;
  }

  public int getTimeout() {
    return timeout;
  }

  public void setTimeout(int timeout) {
    this.timeout = timeout;
  }

  public String getStrictHostKeyChecking() {
    return strictHostKeyChecking;
  }

  public void setStrictHostKeyChecking(String strictHostKeyChecking) {
    this.strictHostKeyChecking = strictHostKeyChecking;
  }

}
