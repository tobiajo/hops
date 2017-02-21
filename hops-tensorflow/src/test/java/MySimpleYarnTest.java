import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.ClientRMProxy;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertEquals;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ConcurrentMap;

public class MySimpleYarnTest {
  MiniYARNCluster cluster;
  YarnConfiguration conf;
  ApplicationClientProtocol appClient;
  
  @Before
  public void setUp() throws Exception {
    cluster = new MiniYARNCluster(MySimpleYarnTest.class.getName(), 1, 1,
        1, 1, false, true);
    conf = new YarnConfiguration();
    cluster.init(conf);
    cluster.start();
    appClient = ClientRMProxy.createRMProxy(conf, ApplicationClientProtocol
        .class, true);
  }
  
  @After
  public void tearDown() throws Exception {
    if (null != appClient) {
      RPC.stopProxy(appClient);
    }
    
    if (null != cluster) {
      cluster.stop();
    }
  }
  
  @Test
  public void testNumberOfNodes() throws Exception {
    // Get the ResourceManager reference
    ResourceManager rm = cluster.getResourceManager();
    int numOfNodes = rm.getResourceScheduler().getNumClusterNodes();
    assertEquals("There should be 1 node in the cluster", 1, numOfNodes);
  }
  
  @Test
  public void testSubmitNewApplication() throws Exception {
    // Get ResourceManager reference
    ResourceManager rm = cluster.getResourceManager();
    
    // Get a new application ID from the RM
    GetNewApplicationResponse newAppRes = appClient.getNewApplication
        (GetNewApplicationRequest.newInstance());
    
    assertNotNull(newAppRes.getApplicationId());
    assertNotNull(newAppRes.getMaximumResourceCapability());
    
    // Construct a dummy launch context for the ApplicationMaster container
    ContainerLaunchContext launchCtx = ContainerLaunchContext.newInstance(
        new HashMap<String, LocalResource>(),
        new HashMap<String, String>(),
        new ArrayList<String>(),
        new HashMap<String, ByteBuffer>(),
        ByteBuffer.allocate(3),
        new HashMap<ApplicationAccessType, String>()
    );
    
    // Construct a dummy Application Submission Context
    ApplicationSubmissionContext submissionCtx =
        ApplicationSubmissionContext.newInstance(
            newAppRes.getApplicationId(),
            "MyFirstApplication",
            "default",
            Priority.newInstance(0),
            launchCtx,
            false,
            true,
            2,
            Resource.newInstance(1024, 1),
            "serviceType",
            false
        );
    
    appClient.submitApplication(SubmitApplicationRequest
        .newInstance(submissionCtx));
    
    // Get the running applications from the RM
    ConcurrentMap<ApplicationId, RMApp> runningApps = rm.getRMContext()
        .getRMApps();
    
    assertEquals("There should be one running up", 1, runningApps.size());
    RMApp rmApp = runningApps.get(newAppRes.getApplicationId());
    String appType = rmApp.getApplicationType();
    assertEquals("Application type should be " + submissionCtx
        .getApplicationType(), submissionCtx.getApplicationType(), appType);
  }
}
