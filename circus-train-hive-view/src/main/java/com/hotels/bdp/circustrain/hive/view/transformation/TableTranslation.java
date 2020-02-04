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
package com.hotels.bdp.circustrain.hive.view.transformation;

import java.util.Objects;

class TableTranslation {

  private static String toUnescapedQualifiedName(String database, String table) {
    return String.format("%s.%s", database, table);
  }

  private static String toEscapedQualifiedName(String database, String table) {
    return String.format("`%s`.`%s`", database, table);
  }

  private static String unescapedTableReference(String table) {
    return String.format("%s.", table);
  }

  private static String escapedTableReference(String table) {
    return String.format("`%s`.", table);
  }

  private final String originalDatabaseName;
  private final String originalTableName;
  private final String replicaDatabaseName;
  private final String replicaTableName;

  TableTranslation(
      String originalDatabaseName,
      String originalTableName,
      String replicaDatabaseName,
      String replicaTableName) {
    this.originalDatabaseName = originalDatabaseName;
    this.originalTableName = originalTableName;
    this.replicaDatabaseName = replicaDatabaseName;
    this.replicaTableName = replicaTableName;
  }

  String toUnescapedQualifiedOriginalName() {
    return toUnescapedQualifiedName(originalDatabaseName, originalTableName);
  }

  String toEscapedQualifiedOriginalName() {
    return toEscapedQualifiedName(originalDatabaseName, originalTableName);
  }

  String toUnescapedQualifiedReplicaName() {
    return toUnescapedQualifiedName(replicaDatabaseName, replicaTableName);
  }

  String toEscapedQualifiedReplicaName() {
    return toEscapedQualifiedName(replicaDatabaseName, replicaTableName);
  }

  String toUnescapedOriginalTableReference() {
    return unescapedTableReference(originalTableName);
  }

  String toEscapedOriginalTableReference() {
    return escapedTableReference(originalTableName);
  }

  String toUnescapedReplicaTableReference() {
    return unescapedTableReference(replicaTableName);
  }

  String toEscapedReplicaTableReference() {
    return escapedTableReference(replicaTableName);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (!(obj instanceof TableTranslation)) {
      return false;
    }
    TableTranslation other = (TableTranslation) obj;
    return Objects.equals(originalDatabaseName, other.originalDatabaseName)
        && Objects.equals(originalTableName, other.originalTableName)
        && Objects.equals(replicaDatabaseName, other.replicaDatabaseName)
        && Objects.equals(replicaTableName, other.replicaTableName);
  }

}
