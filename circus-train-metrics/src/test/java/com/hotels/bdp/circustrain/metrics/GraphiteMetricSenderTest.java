/**
 * Copyright (C) 2016-2021 Expedia, Inc.
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
package com.hotels.bdp.circustrain.metrics;

import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.codahale.metrics.Clock;
import com.codahale.metrics.graphite.GraphiteSender;

@RunWith(MockitoJUnitRunner.class)
public class GraphiteMetricSenderTest {

  @Mock
  private GraphiteSender graphite;
  @Mock
  private Clock clock;

  private GraphiteMetricSender sender;

  @Before
  public void before() {
    when(clock.getTime()).thenReturn(1000L);

    sender = new GraphiteMetricSender(graphite, clock, "prefix");
  }

  @Test
  public void typical() throws IOException {
    when(graphite.isConnected()).thenReturn(false);

    sender.send("name", 2L);

    verify(graphite).connect();
    verify(graphite).send("prefix.name", "2", 1L);
    verify(graphite).flush();
    verify(graphite).close();
  }

  @Test
  public void alreadyConnected() throws IOException {
    when(graphite.isConnected()).thenReturn(true);

    sender.send("name", 2L);

    verify(graphite, never()).connect();
    verify(graphite).send("prefix.name", "2", 1L);
    verify(graphite).flush();
    verify(graphite).close();
  }

  @Test
  public void exceptionOnConnect() throws IOException {
    when(graphite.isConnected()).thenReturn(false);
    doThrow(IOException.class).when(graphite).connect();

    try {
      sender.send("name", 2L);
    } catch (Exception e) {
      fail("Unexpected exception");
    }

    verify(graphite, never()).send("prefix.name", "2", 1L);
    verify(graphite, never()).flush();
    verify(graphite).close();
  }

  @Test
  public void exceptionOnSend() throws IOException {
    when(graphite.isConnected()).thenReturn(true);
    doThrow(IOException.class).when(graphite).send(anyString(), anyString(), anyLong());

    try {
      sender.send("name", 2L);
    } catch (Exception e) {
      fail("Unexpected exception");
    }

    verify(graphite, never()).flush();
    verify(graphite).close();
  }

  @Test
  public void exceptionOnFlush() throws IOException {
    when(graphite.isConnected()).thenReturn(true);
    doThrow(IOException.class).when(graphite).flush();

    try {
      sender.send("name", 2L);
    } catch (Exception e) {
      fail("Unexpected exception");
    }

    verify(graphite).close();
  }

  @Test
  public void exceptionOnClose() throws IOException {
    when(graphite.isConnected()).thenReturn(true);
    doThrow(IOException.class).when(graphite).close();

    try {
      sender.send("name", 2L);
    } catch (Exception e) {
      fail("Unexpected exception");
    }

    verify(graphite).close();
  }

}
