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
package com.hotels.bdp.circustrain.core;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.ExitCodeGenerator;
import org.springframework.context.annotation.Profile;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;

import com.hotels.bdp.circustrain.api.CompletionCode;
import com.hotels.bdp.circustrain.api.Modules;
import com.hotels.bdp.circustrain.api.Replication;
import com.hotels.bdp.circustrain.api.conf.ReplicaCatalog;
import com.hotels.bdp.circustrain.api.conf.Security;
import com.hotels.bdp.circustrain.api.conf.SourceCatalog;
import com.hotels.bdp.circustrain.api.conf.TableReplication;
import com.hotels.bdp.circustrain.api.conf.TableReplications;
import com.hotels.bdp.circustrain.api.event.LocomotiveListener;
import com.hotels.bdp.circustrain.api.event.TableReplicationListener;
import com.hotels.bdp.circustrain.api.metrics.MetricSender;
import com.hotels.bdp.circustrain.core.event.EventUtils;

/**
 * This class is in charge of configuring replications and executing them.
 * <p>
 * This has to be of the highest precedence because <b>Circus Train</b> is not multithread at the moment and each
 * application runner is executed in sequence:
 * <ol>
 * <li>Do replication</li>
 * <li>Remove paths left by old replications (housekeeping)</li>
 * </ol>
 * </p>
 */
@Profile({ Modules.REPLICATION })
@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
class Locomotive implements ApplicationRunner, ExitCodeGenerator {

  private static final Logger LOG = LoggerFactory.getLogger(Locomotive.class);

  private final List<TableReplication> tableReplications;
  private final ReplicationFactory replicationFactory;
  private final MetricSender metricSender;
  private final SourceCatalog sourceCatalog;
  private final ReplicaCatalog replicaCatalog;
  private final Security security;
  private final LocomotiveListener locomotiveListener;
  private final TableReplicationListener tableReplicationListener;
  private long replicationFailures;

  @Autowired
  Locomotive(
      SourceCatalog sourceCatalog,
      ReplicaCatalog replicaCatalog,
      Security security,
      TableReplications tableReplications,
      ReplicationFactory replicationFactory,
      MetricSender metricSender,
      LocomotiveListener locomotiveListener,
      TableReplicationListener tableReplicationListener) {
    this.sourceCatalog = sourceCatalog;
    this.replicaCatalog = replicaCatalog;
    this.security = security;
    this.locomotiveListener = locomotiveListener;
    this.tableReplicationListener = tableReplicationListener;
    this.tableReplications = tableReplications.getTableReplications();
    this.replicationFactory = replicationFactory;
    this.metricSender = metricSender;
  }

  @Override
  public void run(ApplicationArguments args) {
    locomotiveListener.circusTrainStartUp(args.getSourceArgs(), EventUtils.toEventSourceCatalog(sourceCatalog),
        EventUtils.toEventReplicaCatalog(replicaCatalog, security));
    CompletionCode completionCode = CompletionCode.SUCCESS;
    Builder<String, Long> metrics = ImmutableMap.builder();
    replicationFailures = 0;
    long replicated = 0;

    LOG.info("{} tables to replicate.", tableReplications.size());
    for (TableReplication tableReplication : tableReplications) {
      String summary = getReplicationSummary(tableReplication);
      LOG.info("Replicating {} replication mode '{}', strategy '{}'.", summary, tableReplication.getReplicationMode(), tableReplication.getReplicationStrategy());
      try {
        Replication replication = replicationFactory.newInstance(tableReplication);
        tableReplicationListener.tableReplicationStart(EventUtils.toEventTableReplication(tableReplication),
            replication.getEventId());
        replication.replicate();
        LOG.info("Completed replicating: {}.", summary);
        tableReplicationListener.tableReplicationSuccess(EventUtils.toEventTableReplication(tableReplication),
            replication.getEventId());
      } catch (Throwable t) {
        replicationFailures++;
        completionCode = CompletionCode.FAILURE;
        LOG.error("Failed to replicate: {}.", summary, t);
        tableReplicationListener.tableReplicationFailure(EventUtils.toEventTableReplication(tableReplication),
            EventUtils.EVENT_ID_UNAVAILABLE, t);
      }
      replicated++;
    }

    metrics.put("tables_replicated", replicated);
    metrics.put(completionCode.getMetricName(), completionCode.getCode());
    Map<String, Long> metricsMap = metrics.build();
    metricSender.send(metricsMap);
    locomotiveListener.circusTrainShutDown(completionCode, metricsMap);
  }

  @Override
  public int getExitCode() {
    if (replicationFailures == tableReplications.size()) {
      return -1;
    }
    if (replicationFailures > 0) {
      return -2;
    }
    return 0;
  }

  @VisibleForTesting
  String getReplicationSummary(TableReplication tableReplication) {
    return String.format("%s:%s to %s:%s", sourceCatalog.getName(),
        tableReplication.getSourceTable().getQualifiedName(), replicaCatalog.getName(),
        tableReplication.getQualifiedReplicaName());
  }

}
