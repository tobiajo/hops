package old_io.hops.tensorflow;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

public class Client {
  
  private static final Log LOG = LogFactory.getLog(Client.class);
  
  private ClientConf clientConf;
  private Configuration yarnConf;
  private YarnClient yarnClient;
  
  public static void main(String[] args) {
    try {
      LOG.info("Initializing Client");
      new Client(new ClientConf(args)).run();
    } catch (Throwable t) {
      LOG.fatal("Error running Client", t);
    }
  }
  
  public Client(ClientConf clientConf) {
    this(clientConf, new YarnConfiguration());
  }
  
  public Client(ClientConf clientConf, Configuration yarnConf) {
    this.clientConf = clientConf;
    this.yarnConf = yarnConf;
    yarnClient = YarnClient.createYarnClient();
  }
  
  public void run() throws IOException, YarnException {
    LOG.info("Running Client");
    ApplicationReport finalReport =
        monitorApplication(submitApplication(), false);
    LOG.info("Application finished, failed or killed." +
        " yarnAppState=" + finalReport.getYarnApplicationState().toString() +
        ", finalAppStatus=" +
        finalReport.getFinalApplicationStatus().toString());
  }
  
  public void kill(ApplicationId appId) throws IOException, YarnException {
    yarnClient.killApplication(appId);
  }
  
  public ApplicationId submitApplication() throws IOException, YarnException {
    yarnClient.init(yarnConf);
    yarnClient.start();
    
    logClusterInfo(yarnClient);
    
    // Get a new application id
    LOG.info("Requesting a new application from cluster");
    YarnClientApplication newApp = yarnClient.createApplication();
    GetNewApplicationResponse newAppResponse =
        newApp.getNewApplicationResponse();
    
    // Set up contexts
    ContainerLaunchContext containerContext =
        createContainerLaunchContext(newAppResponse);
    ApplicationSubmissionContext appContext =
        createApplicationSubmissionContext(newApp, containerContext);
    
    // Submit
    LOG.info("Submitting application " + newAppResponse.getApplicationId() +
        " to ASM");
    yarnClient.submitApplication(appContext);
    
    return newAppResponse.getApplicationId();
  }
  
  public ApplicationReport monitorApplication(ApplicationId appId,
      boolean returnOnRunning) throws IOException, YarnException {
    while (true) {
      try {
        Thread.sleep(clientConf.getInt(ClientConf.REPORT_INTERVAL));
      } catch (InterruptedException e) {
        LOG.debug("Thread sleep in monitoring loop interrupted");
      }
      
      ApplicationReport report = yarnClient.getApplicationReport(appId);
      logApplicationReport(appId, report);
      
      YarnApplicationState state = report.getYarnApplicationState();
      if (state == YarnApplicationState.FINISHED ||
          state == YarnApplicationState.FAILED ||
          state == YarnApplicationState.KILLED) {
        return report;
      }
      if (returnOnRunning && state == YarnApplicationState.RUNNING) {
        return report;
      }
    }
  }
  
  private ContainerLaunchContext createContainerLaunchContext(
      GetNewApplicationResponse newAppResponse) throws IOException {
    ApplicationId appId = newAppResponse.getApplicationId();
    
    Map<String, LocalResource> localResources = new HashMap<>();
    Utils.addFileToLocalResources(localResources, yarnConf, appId,
        clientConf.get(ClientConf.JAR), "AppMaster.jar");
    
    Map<String, String> environment = new HashMap<>();
    Utils.addClasspathToEnvironment(environment, yarnConf);
    
    // TODO: test if it's possible to add a list of String
    Vector<CharSequence> vargs = new Vector<>();
    vargs.add(ApplicationConstants.Environment.JAVA_HOME.$$() + "/bin/java");
    vargs.add("-Xmx" + clientConf.getInt(ClientConf.AM_MEMORY) + "m");
    vargs.add("old__io.hops.tensorflow.ApplicationMaster");
    if (clientConf.has(ClientConf.DEBUG)) {
      vargs.add("--debug");
    }
    vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR +
        "/AppMaster.stdout");
    vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR +
        "/AppMaster.stderr");
    StringBuilder command = new StringBuilder();
    for (CharSequence str : vargs) {
      command.append(str).append(" ");
    }
    List<String> commands = Collections.singletonList(command.toString());
    LOG.info("Completed setting up app master command " + command.toString());
    
    Map<String, ByteBuffer> serviceData = null;
    ByteBuffer tokens = null;
    Map<ApplicationAccessType, String> acls = null;
    return ContainerLaunchContext
        .newInstance(localResources, environment, commands, serviceData, tokens,
            acls);
  }
  
  
  private ApplicationSubmissionContext createApplicationSubmissionContext(
      YarnClientApplication newApp, ContainerLaunchContext containerContext) {
    ApplicationSubmissionContext appContext =
        newApp.getApplicationSubmissionContext();
    GetNewApplicationResponse newAppResponse =
        newApp.getNewApplicationResponse();
    
    // appContext.setApplicationId();
    appContext.setApplicationName(clientConf.get(ClientConf.APP_NAME));
    appContext.setQueue(clientConf.get(ClientConf.QUEUE_NAME));
    // appContext.setPriority();
    appContext.setAMContainerSpec(containerContext);
    // appContext.setUnmanagedAM();
    // appContext.setCancelTokensWhenComplete();
    // appContext.setMaxAppAttempts();
    
    appContext.setResource(Resource
        .newInstance(getAmMemory(newAppResponse), getAmVCores(newAppResponse)));
    // appContext.setApplicationType();
    // appContext.setKeepContainersAcrossApplicationAttempts();
    // appContext.setApplicationTags();
    // appContext.setNodeLabelExpression();
    // appContext.setAMContainerResourceRequest();
    // appContext.setAttemptFailuresValidityInterval();
    // appContext.setLogAggregationContext();
    // appContext.setReservationID();
    
    return appContext;
  }
  
  private int getAmMemory(GetNewApplicationResponse newAppResponse) {
    int amMemory = clientConf.getInt(ClientConf.AM_MEMORY);
    int maxMem = newAppResponse.getMaximumResourceCapability().getMemory();
    LOG.info("Max mem capabililty of resources in this cluster " + maxMem);
    if (amMemory > maxMem) {
      LOG.info(
          "AM memory specified above max threshold of cluster. Using max value."
              + ", specified=" + amMemory
              + ", max=" + maxMem);
      amMemory = maxMem;
    }
    return amMemory;
  }
  
  private int getAmVCores(GetNewApplicationResponse newAppResponse) {
    int amVCores = clientConf.getInt(ClientConf.AM_VCORES);
    int maxVCores =
        newAppResponse.getMaximumResourceCapability().getVirtualCores();
    LOG.info("Max virtual cores capabililty of resources in this cluster " +
        maxVCores);
    if (amVCores > maxVCores) {
      LOG.info("AM virtual cores specified above max threshold of cluster. "
          + "Using max value." + ", specified=" + amVCores
          + ", max=" + maxVCores);
      amVCores = maxVCores;
    }
    return amVCores;
  }
  
  private void logClusterInfo(YarnClient yarnClient)
      throws IOException, YarnException {
    YarnClusterMetrics clusterMetrics = yarnClient.getYarnClusterMetrics();
    LOG.info("Got Cluster metric info from ASM"
        + ", numNodeManagers=" + clusterMetrics.getNumNodeManagers());
    
    List<NodeReport> clusterNodeReports = yarnClient.getNodeReports(
        NodeState.RUNNING);
    LOG.info("Got Cluster node info from ASM");
    for (NodeReport node : clusterNodeReports) {
      LOG.info("Got node report from ASM for"
          + ", nodeId=" + node.getNodeId()
          + ", nodeAddress" + node.getHttpAddress()
          + ", nodeRackName" + node.getRackName()
          + ", nodeNumContainers" + node.getNumContainers());
    }
    
    QueueInfo queueInfo =
        yarnClient.getQueueInfo(clientConf.get(ClientConf.QUEUE_NAME));
    LOG.info("Queue info"
        + ", queueName=" + queueInfo.getQueueName()
        + ", queueCurrentCapacity=" + queueInfo.getCurrentCapacity()
        + ", queueMaxCapacity=" + queueInfo.getMaximumCapacity()
        + ", queueApplicationCount=" + queueInfo.getApplications().size()
        + ", queueChildQueueCount=" + queueInfo.getChildQueues().size());
    
    List<QueueUserACLInfo> listAclInfo = yarnClient.getQueueAclsInfo();
    for (QueueUserACLInfo aclInfo : listAclInfo) {
      for (QueueACL userAcl : aclInfo.getUserAcls()) {
        LOG.info("User ACL Info for Queue"
            + ", queueName=" + aclInfo.getQueueName()
            + ", userAcl=" + userAcl.name());
      }
    }
  }
  
  private void logApplicationReport(ApplicationId appId,
      ApplicationReport report) {
    LOG.info("Got application report from ASM for"
        + ", appId=" + appId.getId()
        + ", clientToAMToken=" + report.getClientToAMToken()
        + ", appDiagnostics=" + report.getDiagnostics()
        + ", appMasterHost=" + report.getHost()
        + ", appQueue=" + report.getQueue()
        + ", appMasterRpcPort=" + report.getRpcPort()
        + ", appStartTime=" + report.getStartTime()
        + ", yarnAppState=" + report.getYarnApplicationState().toString()
        + ", finalAppStatus=" + report.getFinalApplicationStatus().toString()
        + ", appTrackingUrl=" + report.getTrackingUrl()
        + ", appUser=" + report.getUser());
  }
}
