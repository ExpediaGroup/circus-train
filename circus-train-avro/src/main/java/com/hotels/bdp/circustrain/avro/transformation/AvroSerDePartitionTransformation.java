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
package com.hotels.bdp.circustrain.avro.transformation;

import static com.hotels.bdp.circustrain.avro.util.AvroStringUtils.argsPresent;
import static com.hotels.bdp.circustrain.avro.util.AvroStringUtils.avroDestination;

import org.apache.hadoop.hive.metastore.api.Partition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import com.hotels.bdp.circustrain.api.Modules;
import com.hotels.bdp.circustrain.api.metadata.PartitionTransformation;
import com.hotels.bdp.circustrain.avro.conf.AvroSerDeConfig;
import com.hotels.bdp.circustrain.avro.hive.HiveObjectUtils;
import com.hotels.bdp.circustrain.avro.util.SchemaCopier;

@Profile({ Modules.REPLICATION })
@Component
public class AvroSerDePartitionTransformation extends AbstractAvroSerDeTransformation
    implements PartitionTransformation {

  private final SchemaCopier copier;

  @Autowired
  public AvroSerDePartitionTransformation(AvroSerDeConfig avroSerDeConfig, SchemaCopier copier) {
    super(avroSerDeConfig);
    this.copier = copier;
  }

  @Override
  public Partition transform(Partition partition) {
    if (avroTransformationSpecified()) {
      partition = apply(partition, avroDestination(getAvroSchemaDestinationFolder(), getEventId(), getTableLocation()));
    }
    return partition;
  }

  private Partition apply(Partition partition, String avroSchemaDestination) {
    String source = HiveObjectUtils.getParameter(partition, AVRO_SCHEMA_URL_PARAMETER);
    if (argsPresent(source, avroSchemaDestination)) {
      String destinationPath = copier.copy(source, avroSchemaDestination).toString();
      HiveObjectUtils.updateSerDeUrl(partition, AVRO_SCHEMA_URL_PARAMETER, destinationPath);
    }
    return partition;
  }
}
