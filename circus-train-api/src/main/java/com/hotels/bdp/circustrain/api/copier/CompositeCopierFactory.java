/**
 * Copyright (C) 2016-2020 Expedia, Inc.
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
package com.hotels.bdp.circustrain.api.copier;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;

import com.google.common.collect.ImmutableList;

import com.hotels.bdp.circustrain.api.CircusTrainException;
import com.hotels.bdp.circustrain.api.metrics.Metrics;

public class CompositeCopierFactory implements CopierFactory {

  private static class CompositeCopier implements Copier {

    private final List<Copier> copiers;
    private final MetricsMerger metricsMerger;

    private CompositeCopier(List<Copier> copiers, MetricsMerger metricsMerger) {
      this.copiers = ImmutableList.copyOf(copiers);
      this.metricsMerger = metricsMerger;
    }

    @Override
    public Metrics copy() throws CircusTrainException {
      Metrics metrics = Metrics.NULL_VALUE;
      for (Copier copier : copiers) {
        Metrics copierMetrics = copier.copy();
        if (copierMetrics == null) {
          continue;
        }
        metrics = metricsMerger.merge(metrics, copierMetrics);
      }
      return metrics;
    }

  }

  private final List<CopierFactory> delegates;
  private final CopierPathGenerator pathGenerator;
  private final MetricsMerger metricsMerger;

  public CompositeCopierFactory(List<CopierFactory> delegates) {
    this(delegates, CopierPathGenerator.IDENTITY, MetricsMerger.DEFAULT);
  }

  public CompositeCopierFactory(
      List<CopierFactory> delegates,
      CopierPathGenerator pathGenerator,
      MetricsMerger metricsMerger) {
    checkArgument(delegates != null && !delegates.isEmpty(), "At least one delegate is required");
    checkNotNull(pathGenerator, "pathGenerator is required");
    checkNotNull(metricsMerger, "metricsMerger is required");
    this.delegates = delegates;
    this.pathGenerator = pathGenerator;
    this.metricsMerger = metricsMerger;
  }

  @Override
  public boolean supportsSchemes(String sourceScheme, String replicaScheme) {
    return delegates.get(0).supportsSchemes(sourceScheme, replicaScheme);
  }

  @Override
  public Copier newInstance(CopierContext copierContext) {
    List<Copier> copiers = new ArrayList<>(delegates.size());
    int i = 0;
    for (CopierFactory delegate : delegates) {
      CopierPathGeneratorParams copierPathGeneratorParams = CopierPathGeneratorParams
          .newParams(i++, copierContext.getEventId(), copierContext.getSourceBaseLocation(),
              copierContext.getSourceSubLocations(), copierContext.getReplicaLocation(),
              copierContext.getCopierOptions());
      Path newSourceBaseLocation = pathGenerator.generateSourceBaseLocation(copierPathGeneratorParams);
      Path newReplicaLocation = pathGenerator.generateReplicaLocation(copierPathGeneratorParams);

      CopierContext delegateContext = new CopierContext(copierContext.getEventId(), newSourceBaseLocation,
          copierContext.getSourceSubLocations(), newReplicaLocation, copierContext.getCopierOptions());
      Copier copier = delegate.newInstance(delegateContext);
      copiers.add(copier);
    }
    return new CompositeCopier(copiers, metricsMerger);
  }

  @Override
  public Copier newInstance(
      String eventId,
      Path sourceBaseLocation,
      Path replicaLocation,
      Map<String, Object> copierOptions) {
    CopierContext copierContext = new CopierContext(eventId, sourceBaseLocation, replicaLocation, copierOptions);
    return newInstance(copierContext);
  }

  @Override
  public Copier newInstance(
      String eventId,
      Path sourceBaseLocation,
      List<Path> sourceSubLocations,
      Path replicaLocation,
      Map<String, Object> copierOptions) {
    CopierContext copierContext = new CopierContext(eventId, sourceBaseLocation, sourceSubLocations, replicaLocation,
        copierOptions);
    return newInstance(copierContext);
  }

}
