package io.hops.tensorflow;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Records;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import static io.hops.tensorflow.ClientConf.*;

public class Client {

    private static final Log LOG = LogFactory.getLog(Client.class);

    private ClientConf clientConf;
    private Configuration yarnConf;

    public static void main(String[] args) {
        try {
            new Client(new ClientConf(args), new YarnConfiguration()).run();
        } catch (Throwable e) {
            e.printStackTrace();
        }
    }

    public Client(ClientConf clientConf, Configuration yarnConf) {
        this.clientConf = clientConf;
        this.yarnConf = yarnConf;
    }

    public void run() throws IOException, YarnException {
        monitorApplication(submitApplication());
    }

    protected ApplicationId submitApplication() throws IOException, YarnException {
        YarnClient yarnClient = YarnClient.createYarnClient();
        yarnClient.init(yarnConf);
        yarnClient.start();

        // Get a new application id
        YarnClientApplication newApp = yarnClient.createApplication();
        GetNewApplicationResponse newAppResponse = newApp.getNewApplicationResponse();

        // Set up contexts
        ContainerLaunchContext containerContext = createContainerLaunchContext(newAppResponse);
        ApplicationSubmissionContext appContext = createApplicationSubmissionContext(newApp, containerContext);

        // Submit
        yarnClient.submitApplication(appContext);

        return newAppResponse.getApplicationId();
    }

    protected void monitorApplication(ApplicationId appId) {
        // TODO: add monitoring, test it too
    }

    private ContainerLaunchContext createContainerLaunchContext(GetNewApplicationResponse newAppResponse) {
        ContainerLaunchContext containerContext = Records.newRecord(ContainerLaunchContext.class);
        containerContext.setTokens(null);
        containerContext.setLocalResources(new HashMap<String, LocalResource>());
        containerContext.setServiceData(null);
        containerContext.setEnvironment(new HashMap<String, String>());
        containerContext.setCommands(new ArrayList<String>());
        containerContext.setApplicationACLs(null);
        return containerContext;
    }

    private ApplicationSubmissionContext createApplicationSubmissionContext(YarnClientApplication newApp, ContainerLaunchContext containerContext) {
        ApplicationSubmissionContext appContext = newApp.getApplicationSubmissionContext();
        // appContext.setApplicationId();
        appContext.setApplicationName(clientConf.get(APP_NAME));
        // appContext.setQueue();
        // appContext.setPriority();
        appContext.setAMContainerSpec(containerContext);
        // appContext.setUnmanagedAM();
        // appContext.setCancelTokensWhenComplete();
        // appContext.setMaxAppAttempts();
        appContext.setResource(Resource.newInstance(clientConf.getInt(AM_MEMORY), clientConf.getInt(AM_VCORES)));
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
}
