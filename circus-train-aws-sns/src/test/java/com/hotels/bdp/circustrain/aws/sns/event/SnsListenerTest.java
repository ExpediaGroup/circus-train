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
package com.hotels.bdp.circustrain.aws.sns.event;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.amazonaws.services.sns.AmazonSNSAsyncClient;
import com.amazonaws.services.sns.model.PublishRequest;

import com.hotels.bdp.circustrain.api.event.EventPartition;
import com.hotels.bdp.circustrain.api.event.EventReplicaCatalog;
import com.hotels.bdp.circustrain.api.event.EventSourceCatalog;
import com.hotels.bdp.circustrain.api.event.EventSourceTable;
import com.hotels.bdp.circustrain.api.event.EventTableReplication;
import com.hotels.bdp.circustrain.api.metrics.Metrics;

@RunWith(MockitoJUnitRunner.class)
public class SnsListenerTest {

  private static final String SUBJECT = "choochoo";
  private static final RuntimeException ERROR = new RuntimeException("error message");
  private static final String ENDTIME = "endtime";
  private static List<EventPartition> PARTITIONS_0;
  private static List<EventPartition> PARTITIONS_1;
  private static final String STARTTIME = "starttime";
  private static final String EVENT_ID = "EVENT_ID";

  static {
    try {
      PARTITIONS_0 = Arrays.asList(new EventPartition(Arrays.asList("2014-01-01", "0"), new URI("location_0")));
      PARTITIONS_1 = Arrays.asList(new EventPartition(Arrays.asList("2014-01-01", "1"), new URI("location_1")));
    } catch (URISyntaxException e) {}
  }
  @Mock
  private ListenerConfig config;
  @Mock
  private AmazonSNSAsyncClient client;
  @Mock
  private EventSourceCatalog sourceCatalog;
  @Mock
  private EventReplicaCatalog replicaCatalog;
  @Mock
  private EventSourceTable sourceTable;
  @Mock
  private EventTableReplication tableReplication;
  @Mock
  private Metrics metrics;
  @Mock
  private Clock clock;
  @Captor
  private ArgumentCaptor<PublishRequest> requestCaptor;

  @Before
  public void prepare() throws URISyntaxException {
    when(clock.getTime()).thenReturn(STARTTIME, ENDTIME);

    Map<String, String> headers = new HashMap<>();
    headers.put("pipeline-id", "0943879438");
    when(config.getStartTopic()).thenReturn("startArn");
    when(config.getSuccessTopic()).thenReturn("successArn");
    when(config.getFailTopic()).thenReturn("failArn");
    when(config.getTopic()).thenReturn("topicArn");
    when(config.getSubject()).thenReturn(SUBJECT);
    when(config.getHeaders()).thenReturn(headers);
    when(config.getQueueSize()).thenReturn(10);

    when(tableReplication.getSourceTable()).thenReturn(sourceTable);
    when(tableReplication.getQualifiedReplicaName()).thenReturn("replicaDb.replicaTable");
    when(metrics.getBytesReplicated()).thenReturn(40L);
    when(sourceCatalog.getName()).thenReturn("sourceCatalogName");
    when(replicaCatalog.getName()).thenReturn("replicaCatalogName");
    when(sourceTable.getQualifiedName()).thenReturn("srcDb.srcTable");
  }

  @Test
  public void start() {
    SnsListener listener = new SnsListener(client, config, clock);
    listener.circusTrainStartUp(new String[] {}, sourceCatalog, replicaCatalog);
    listener.tableReplicationStart(tableReplication, EVENT_ID);

    verify(client).publish(requestCaptor.capture());
    PublishRequest request = requestCaptor.getValue();
    assertThat(request.getSubject(), is(SUBJECT));
    assertThat(request.getTopicArn(), is("startArn"));
    assertThat(request.getMessage(),
        is("{\"protocolVersion\":\"1.0\",\"type\":\"START\",\"headers\":{\"pipeline-id\":\"0943879438\"},"
            + "\"startTime\":\"starttime\",\"eventId\":\"EVENT_ID\",\"sourceCatalog\":\"sourceCatalogName\","
            + "\"replicaCatalog\":\"replicaCatalogName\",\"sourceTable\":\"srcDb.srcTable\",\"replicaTable\":"
            + "\"replicaDb.replicaTable\"}"));
  }

  @Test
  public void success() {
    SnsListener listener = new SnsListener(client, config, clock);
    listener.circusTrainStartUp(new String[] {}, sourceCatalog, replicaCatalog);
    listener.tableReplicationStart(tableReplication, EVENT_ID);
    listener.partitionsToAlter(PARTITIONS_0);
    listener.partitionsToCreate(PARTITIONS_1);
    listener.copierEnd(metrics);
    listener.tableReplicationSuccess(tableReplication, EVENT_ID);

    verify(client, times(2)).publish(requestCaptor.capture());
    PublishRequest request = requestCaptor.getAllValues().get(1);
    assertThat(request.getSubject(), is(SUBJECT));
    assertThat(request.getTopicArn(), is("successArn"));
    assertThat(request.getMessage(),
        is("{\"protocolVersion\":\"1.0\",\"type\":\"SUCCESS\",\"headers\":{\"pipeline-id\":\"0943879438\"},"
            + "\"startTime\":\"starttime\",\"endTime\":\"endtime\",\"eventId\":\"EVENT_ID\",\"sourceCatalog\""
            + ":\"sourceCatalogName\",\"replicaCatalog\":\"replicaCatalogName\",\"sourceTable\":"
            + "\"srcDb.srcTable\",\"replicaTable\":\"replicaDb.replicaTable\",\"modifiedPartitions\":"
            + "[[\"2014-01-01\",\"0\"],[\"2014-01-01\",\"1\"]],\"bytesReplicated\":40}"));
  }

  @Test
  public void failure() {
    SnsListener listener = new SnsListener(client, config, clock);
    listener.circusTrainStartUp(new String[] {}, sourceCatalog, replicaCatalog);
    listener.tableReplicationStart(tableReplication, EVENT_ID);
    listener.partitionsToAlter(PARTITIONS_0);
    listener.partitionsToCreate(PARTITIONS_1);
    listener.copierEnd(metrics);
    listener.tableReplicationFailure(tableReplication, EVENT_ID, ERROR);

    verify(client, times(2)).publish(requestCaptor.capture());
    PublishRequest request = requestCaptor.getValue();
    assertThat(request.getSubject(), is(SUBJECT));
    assertThat(request.getTopicArn(), is("failArn"));
    assertThat(request.getMessage(), is("{\"protocolVersion\":\"1.0\",\"type\":\"FAILURE\",\"headers\":"
        + "{\"pipeline-id\":\"0943879438\"},\"startTime\":\"starttime\",\"endTime\":\"endtime\",\"eventId\":"
        + "\"EVENT_ID\",\"sourceCatalog\":\"sourceCatalogName\",\"replicaCatalog\":\"replicaCatalogName\","
        + "\"sourceTable\":\"srcDb.srcTable\",\"replicaTable\":\"replicaDb.replicaTable\",\"modifiedPartitions\":"
        + "[[\"2014-01-01\",\"0\"],[\"2014-01-01\",\"1\"]],\"bytesReplicated\":40,\"errorMessage\":\"error message\"}"));
  }

  @Test
  public void failureBeforeTableReplicationStartIsCalled() {
    SnsListener listener = new SnsListener(client, config, clock);
    listener.circusTrainStartUp(new String[] {}, sourceCatalog, replicaCatalog);
    listener.tableReplicationFailure(tableReplication, EVENT_ID, ERROR);

    verify(client, times(1)).publish(requestCaptor.capture());
    PublishRequest request = requestCaptor.getValue();
    assertThat(request.getSubject(), is(SUBJECT));
    assertThat(request.getTopicArn(), is("failArn"));
    assertThat(request.getMessage(), is("{\"protocolVersion\":\"1.0\",\"type\":\"FAILURE\",\"headers\":"
        + "{\"pipeline-id\":\"0943879438\"},\"startTime\":\"starttime\",\"endTime\":\"endtime\",\"eventId\":"
        + "\"EVENT_ID\",\"sourceCatalog\":\"sourceCatalogName\",\"replicaCatalog\":\"replicaCatalogName\","
        + "\"sourceTable\":\"srcDb.srcTable\",\"replicaTable\":\"replicaDb.replicaTable\",\"bytesReplicated\":0,\"errorMessage\":\"error message\"}"));
  }

  @Test
  public void getModifiedPartitionsTypical() throws URISyntaxException {
    List<EventPartition> created = Arrays.asList(new EventPartition(Arrays.asList("a"), new URI("location_a")));
    List<EventPartition> altered = Arrays.asList(new EventPartition(Arrays.asList("b"), new URI("location_b")));
    List<List<String>> partitions = SnsListener.getModifiedPartitions(created, altered);
    assertThat(partitions.size(), is(2));
    assertThat(partitions.get(0), is(Arrays.asList("a")));
    assertThat(partitions.get(1), is(Arrays.asList("b")));
  }

  @Test
  public void getModifiedPartitionsOneOnly() throws URISyntaxException {
    List<EventPartition> created = Arrays.asList(new EventPartition(Arrays.asList("a"), new URI("location_a")),
        new EventPartition(Arrays.asList("b"), new URI("location_b")));
    List<List<String>> partitions = SnsListener.getModifiedPartitions(created, null);
    assertThat(partitions.size(), is(2));
    assertThat(partitions.get(0), is(Arrays.asList("a")));
    assertThat(partitions.get(1), is(Arrays.asList("b")));
  }

  @Test
  public void getModifiedPartitionsBothNull() throws URISyntaxException {
    List<List<String>> partitions = SnsListener.getModifiedPartitions(null, null);
    assertThat(partitions, is(nullValue()));
  }

}
