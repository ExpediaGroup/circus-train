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
package com.hotels.bdp.circustrain.housekeeping.audit;

import javax.persistence.Entity;
import javax.persistence.Table;

import org.hibernate.envers.RevisionEntity;

import com.hotels.housekeeping.audit.EntityAuditRevision;

@Entity
@RevisionEntity
@Table(schema = "circus_train", name = "audit_revision")
public class CircusTrainAuditRevision extends EntityAuditRevision {
}
