/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.hops.tensorflow;

import io.hops.tensorflow.ClusterSpecGenServer;
import old__io.hops.tensorflow.ClusterSpecGenClient;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class TestClusterSpecGen {
  
  private static final int INITIAL_PORT = 1;
  private static final int NUM_CONTAINERS = 3;
  
  private ClusterSpecGenServer server;
  private ClusterSpecGenClient client;
  
  @Before
  public void setup() {
    Thread thread = new Thread(new Runnable() {
      @Override
      public void run() {
        
      }
    });
    server = new ClusterSpecGenServer(NUM_CONTAINERS);
    int port = INITIAL_PORT;
    while (port <= 65535) {
      try {
        server.start(port);
        client = new ClusterSpecGenClient("localhost", port);
        break;
      } catch (IOException e) {
        port++;
      }
    }
  }
  
  @After
  public void tearDown() throws Exception {
    server.stop();
    if (client != null) {
      client.shutdown();
    }
  }
  
  @Test
  public void ClusterSpecGenTest() {
    Assert.assertTrue(client.registerContainer("A1", "C1", "(ip)", 1024, "ps", 0));
    Assert.assertTrue(client.registerContainer("A1", "C1", "(ip)", 1024, "ps", 0));
    Assert.assertEquals(0, client.getClusterSpec().size());
    Assert.assertTrue(client.registerContainer("A1", "C2", "(ip)", 1024, "worker", 0));
    Assert.assertTrue(client.registerContainer("A1", "C3", "(ip)", 1024, "worker", 1));
    Assert.assertEquals(NUM_CONTAINERS, client.getClusterSpec().size());
  }
}
