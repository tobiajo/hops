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
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

public class TestYarnTF extends TestCluster {
  
  private static final Log LOG = LogFactory.getLog(TestYarnTF.class);
  
  @Test(timeout=90000)
  public void testYarnTFWithShellScript() throws Exception {
    final File basedir = new File("target", TestYarnTF.class.getName());
    final File tmpDir = new File(basedir, "tmpDir");
    tmpDir.mkdirs();
    final File customShellScript = new File(tmpDir, "custom_script.sh");
    if (customShellScript.exists()) {
      customShellScript.delete();
    }
    if (!customShellScript.createNewFile()) {
      Assert.fail("Can not create custom shell script file.");
    }
    PrintWriter fileWriter = new PrintWriter(customShellScript);
    // set the output to DEBUG level
    fileWriter.write("echo \"amazing output: $1 $2\"");
    fileWriter.close();
    System.out.println(customShellScript.getAbsolutePath());
    String[] args = {
        "--jar",
        APPMASTER_JAR,
        "--num_containers",
        "1",
        "--shell_script",
        customShellScript.getAbsolutePath(),
        "--master_memory",
        "512",
        "--master_vcores",
        "2",
        "--container_memory",
        "128",
        "--container_vcores",
        "1",
        "--main",
        customShellScript.getAbsolutePath(),
        "--shell_args",
        "hello",
        "--shell_args",
        "world"
    };
    
    LOG.info("Initializing DS Client");
    final Client client =
        new Client(new Configuration(yarnCluster.getConfig()));
    boolean initSuccess = client.init(args);
    Assert.assertTrue(initSuccess);
    LOG.info("Running DS Client");
    ApplicationId appId = client.submitApplication();
    boolean result = client.monitorApplication(appId);
    LOG.info("Client run completed. Result=" + result);
    //List<String> expectedContent = new ArrayList<String>();
    //expectedContent.add("testDSShellWithShellScript");
    //TestUtils.verifyContainerLog(yarnCluster, 1, expectedContent, false, "");
    Assert.assertTrue(TestUtils.dumpAllRemoteContainersLogs(yarnCluster, appId));
    Assert.assertFalse(TestUtils.dumpAllAggregatedContainersLogs(yarnCluster, appId));;
    Thread.sleep(5000);
    Assert.assertFalse(TestUtils.dumpAllRemoteContainersLogs(yarnCluster, appId));
    Assert.assertTrue(TestUtils.dumpAllAggregatedContainersLogs(yarnCluster, appId));
  }
}
