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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;

public class DebugYarnTF extends YarnCluster {
  
  private static final Log LOG = LogFactory.getLog(DebugYarnTF.class);
  
  @Test
  public void investigateRM() throws Exception {
    String[] args = {
        "--jar",
        APPMASTER_JAR,
        "--num_containers",
        "8",
        "--shell_command",
        Shell.WINDOWS ? "dir" : "ls",
        "--master_memory",
        "512",
        "--master_vcores",
        "2",
        "--container_memory",
        "128",
        "--container_vcores",
        "8"
    };
    
    LOG.info("Initializing YarnTF Client");
    final Client client =
        new Client(new Configuration(yarnCluster.getConfig()));
    boolean initSuccess = client.init(args);
    Assert.assertTrue(initSuccess);
    LOG.info("Submitting YarnTF Client");
    final ApplicationId appId = client.submitApplication();
    Assert.assertNotNull(appId);
    
    final AtomicBoolean killed = new AtomicBoolean(false);
    new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          YarnClient yarnClient = YarnClient.createYarnClient();
          yarnClient.init(new Configuration(yarnCluster.getConfig()));
          yarnClient.start();
          Thread.sleep(30000);
          LOG.info("Timeout, killing application");
          yarnClient.killApplication(appId);
          killed.set(true);
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    }).start();
    
    LOG.info("Monitoring YarnTF Client");
    boolean runSuccess = client.monitorApplication(appId);
    
    LOG.info("Dumping logs");
    if (killed.get()) {
      while (!dumpAggregatedContainerLogs(appId)) {
        Thread.sleep(100);
      }
    } else {
      dumpRemoteContainersLogs(appId);
    }
    
    Assert.assertTrue(runSuccess);
  }
}
