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
package com.hotels.bdp.circustrain.housekeeping.model;

import java.io.Serializable;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.PrePersist;
import javax.persistence.Table;
import javax.persistence.UniqueConstraint;

import org.hibernate.envers.Audited;
import org.joda.time.Instant;

import com.google.common.base.Objects;

@Entity
// @Converter doesn't work with @Audited until https://hibernate.atlassian.net/browse/HHH-9042 is released
@Audited
@Table(schema = "circus_train", name = "Legacy_replica_path",
    uniqueConstraints = @UniqueConstraint(columnNames = { "path", "creation_timestamp" }))
public class LegacyReplicaPath implements Serializable {
  private static final long serialVersionUID = 1L;

  @Id
  @GeneratedValue
  @Column(name = "id")
  private long id;

  @Column(name = "event_id", nullable = false, length = 250)
  private String eventId;

  @Column(name = "path", nullable = false, length = 10000)
  private String path;

  @Column(name = "creation_timestamp", nullable = false, updatable = false)
  private long creationTimestamp;

  @Column(name = "path_event_id", nullable = true, length = 250)
  private String pathEventId;

  protected LegacyReplicaPath() {}

  public LegacyReplicaPath(String eventId, String pathEventId, String path) {
    this.eventId = eventId;
    this.pathEventId = pathEventId;
    this.path = path;
  }

  public long getId() {
    return id;
  }

  public String getEventId() {
    return eventId;
  }

  public String getPathEventId() {
    return pathEventId;
  }

  public String getPath() {
    return path;
  }

  public long getCreationTimestamp() {
    return creationTimestamp;
  }

  public void setPathEventId(String pathEventId) {
    this.pathEventId = pathEventId;
  }

  protected void setCreationTimestamp(long creationTimestamp) {
    this.creationTimestamp = creationTimestamp;
  }

  @PrePersist
  private void onPersist() {
    setCreationTimestamp(new Instant().getMillis());
  }

  @Override
  public String toString() {
    return Objects
        .toStringHelper(this)
        .add("id", id)
        .add("eventId", eventId)
        .add("pathEventId", pathEventId)
        .add("path", path)
        .add("creationTimestamp", creationTimestamp)
        .toString();
  }
}
