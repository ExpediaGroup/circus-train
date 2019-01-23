/**
 * Copyright (C) 2016-2019 Expedia Inc.
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
package com.hotels.bdp.circustrain.common.test.base;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import javax.annotation.Nonnull;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableMap;

import com.hotels.bdp.circustrain.CircusTrain;
import com.hotels.bdp.circustrain.common.test.xml.HiveXml;

public class CircusTrainRunner {

  private final String databaseName;
  private final File sourceWarehouseUri;
  private final File replicaWarehouseUri;
  private final String replicaConnectionUri;
  private final String graphiteUri;
  private final HiveXml hiveXml;
  private final Map<String, String> sourceConfigurationProperties;
  private final Map<String, String> replicaConfigurationProperties;
  private final Map<String, String> copierOptions;

  private final File housekeepingDbDirectory;
  private final File housekeepingDbLocation;

  public static class Builder {
    private final String databaseName;
    private final File sourceWarehouseUri;
    private final File replicaWarehouseUri;
    private final File housekeepingDbLocation;
    private String replicaConnectionUri;
    private String sourceThriftConnectionUri;
    private String sourceConnectionURL;
    private String sourceDriverClassName;
    private String graphiteUri;
    private final Map<String, String> sourceConfigurationProperties = new HashMap<>();
    private final Map<String, String> replicaConfigurationProperties = new HashMap<>();
    private final Map<String, String> copierOptions = new HashMap<>();

    private Builder(
        String databaseName,
        File sourceWarehouseUri,
        File replicaWarehouseUri,
        File housekeepingDbLocation) {
      this.databaseName = databaseName;
      this.sourceWarehouseUri = sourceWarehouseUri;
      this.replicaWarehouseUri = replicaWarehouseUri;
      this.housekeepingDbLocation = housekeepingDbLocation;
    }

    public Builder sourceMetaStore(
        String sourceThriftConnectionUri,
        String sourceConnectionURL,
        String sourceDriverClassName) {
      this.sourceThriftConnectionUri = sourceThriftConnectionUri;
      this.sourceConnectionURL = sourceConnectionURL;
      this.sourceDriverClassName = sourceDriverClassName;
      return this;
    }

    public Builder replicaMetaStore(String replicaConnectionUri) {
      this.replicaConnectionUri = replicaConnectionUri;
      return this;
    }

    public Builder sourceConfigurationProperty(String key, String value) {
      sourceConfigurationProperties.put(key, value);
      return this;
    }

    public Builder replicaConfigurationProperty(String key, String value) {
      replicaConfigurationProperties.put(key, value);
      return this;
    }

    public Builder graphiteUri(String graphiteUri) {
      this.graphiteUri = graphiteUri;
      return this;
    }

    public Builder copierOption(String key, String value) {
      copierOptions.put(key, value);
      return this;
    }

    public CircusTrainRunner build() {
      checkArgument(!isNullOrEmpty(databaseName));
      checkArgument(sourceWarehouseUri != null);
      checkArgument(replicaWarehouseUri != null);
      checkArgument(housekeepingDbLocation != null);
      checkArgument(!isNullOrEmpty(replicaConnectionUri));

      HiveXml hiveXml = new HiveXml(sourceThriftConnectionUri, sourceConnectionURL, sourceDriverClassName);
      hiveXml.create();

      CircusTrainRunner ctRunner = new CircusTrainRunner(databaseName, hiveXml, sourceWarehouseUri, replicaWarehouseUri,
          replicaConnectionUri, sourceConfigurationProperties, replicaConfigurationProperties, copierOptions,
          graphiteUri, housekeepingDbLocation);

      return ctRunner;
    }
  }

  public static Builder builder(
      String databaseName,
      File sourceWarehouseUri,
      File replicaWarehouseUri,
      File housekeepingDbLocation) {
    return new Builder(databaseName, sourceWarehouseUri, replicaWarehouseUri, housekeepingDbLocation);
  }

  private CircusTrainRunner(
      String databaseName,
      HiveXml hiveXml,
      File sourceWarehouseUri,
      File replicaWarehouseUri,
      String replicaConnectionUri,
      Map<String, String> sourceConfigurationProperties,
      Map<String, String> replicaConfigurationProperties,
      Map<String, String> copierOptions,
      String graphiteUri,
      File housekeepingDbLocation) {
    this.databaseName = databaseName;
    this.sourceWarehouseUri = sourceWarehouseUri;
    this.replicaWarehouseUri = replicaWarehouseUri;
    this.replicaConnectionUri = replicaConnectionUri;
    this.sourceConfigurationProperties = ImmutableMap.copyOf(sourceConfigurationProperties);
    this.replicaConfigurationProperties = ImmutableMap.copyOf(replicaConfigurationProperties);
    this.copierOptions = ImmutableMap.copyOf(copierOptions);
    this.graphiteUri = graphiteUri;
    this.hiveXml = hiveXml;
    this.housekeepingDbLocation = housekeepingDbLocation;
    housekeepingDbDirectory = housekeepingDbLocation.getParentFile();
  }

  public File sourceWarehouseLocation() {
    return sourceWarehouseUri;
  }

  public File replicaWarehouseLocation() {
    return replicaWarehouseUri;
  }

  public File housekeepingDbDirectory() {
    return housekeepingDbDirectory;
  }

  public File housekeepingDbLocation() {
    return housekeepingDbLocation;
  }

  private Map<String, String> populateProperties(String configFilePath, String... modules) {
    File configFile = new File(configFilePath);
    ImmutableMap.Builder<String, String> builder = ImmutableMap
        .<String, String>builder()
        // Configuration file
        .put("config", configFile.getAbsolutePath())
        .put("config-location", configFile.getParent())
        // Logging
        .put("logging.config", "classpath:test-log4j.xml")
        // Spring
        .put("spring.jpa.show-sql", "true")
        // Replica Catalog
        .put("replica-catalog.name", "replica-" + databaseName)
        .put("replica-catalog.hive-metastore-uris", replicaConnectionUri)
        .putAll(prefixProperties("replica-catalog.configuration-properties", replicaConfigurationProperties))
        // Copier properties
        .putAll(prefixProperties("copier-options", copierOptions))
        // Source catalog
        .put("source-catalog.name", "source-" + databaseName)
        .put("source-catalog.disable-snapshots", "true")
        .put("source-catalog.site-xml", hiveXml.getFileName())
        .putAll(prefixProperties("source-catalog.configuration-properties", sourceConfigurationProperties))
        // Housekeeping
        .put("spring.datasource.schema", "classpath:/schema.sql,schema-tables.sql")
        .put("spring.datasource.max-active", "2")
        .put("housekeeping.h2.database", housekeepingDbLocation.getAbsolutePath())
        // Table Replications helper variable
        .put("circus-train-runner.database-name", databaseName)
        .put("circus-train-runner.source-warehouse-uri", sourceWarehouseUri.toURI().toString())
        .put("circus-train-runner.replica-warehouse-uri", replicaWarehouseUri.toURI().toString())
        .put("circus-train-runner.housekeeping-db-directory", housekeepingDbDirectory.toURI().toString());
    // Graphite
    if (graphiteUri != null) {
      builder.put("graphite.host", graphiteUri);
    }
    // Modules
    if (modules != null && modules.length > 0) {
      builder.put("modules", Joiner.on(",").join(modules));
    }
    return builder.build();
  }

  private Map<String, String> prefixProperties(String prefix, Map<String, String> properties) {
    ImmutableMap.Builder<String, String> builder = ImmutableMap.<String, String>builder();
    for (Entry<String, String> property : properties.entrySet()) {
      String newKey = new StringBuilder(prefix).append(".").append(property.getKey()).toString();
      builder.put(newKey, property.getValue());
    }
    return builder.build();
  }

  private static String[] getArgsArray(Map<String, String> props) {
    String[] args = FluentIterable.from(props.entrySet()).transform(new Function<Entry<String, String>, String>() {
      @Override
      public String apply(@Nonnull Entry<String, String> input) {
        return "--" + input.getKey() + "=" + input.getValue();
      }
    }).toArray(String.class);
    return args;
  }

  public void run(String configFilePath, String... modules) throws Exception {
    Map<String, String> props = populateProperties(configFilePath, modules);
    CircusTrain.main(getArgsArray(props));
  }

}
