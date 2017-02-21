package old_io.hops.tensorflow;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class ApplicationMaster {

    private static final Log LOG = LogFactory.getLog(ApplicationMaster.class);

    private ApplicationMasterConf amConf;
    private Configuration yarnConf;

    private AMRMClientAsync rmClient;
    private NMClient/*Async*/ nmClient;

    private AtomicInteger numCompletedContainers = new AtomicInteger();
    private AtomicInteger numAllocatedContainers = new AtomicInteger();
    private AtomicInteger numFailedContainers = new AtomicInteger();
    private AtomicInteger numRequestedContainers = new AtomicInteger();

    private AtomicBoolean complete = new AtomicBoolean(false);

    private List<Thread> launchThreads = new ArrayList<>();

    public static void main(String[] args) {
        try {
            LOG.info("Initializing ApplicationMaster");
            new ApplicationMaster(new ApplicationMasterConf(args)).run();
        } catch (Throwable t) {
            LOG.fatal("Error running ApplicationMaster", t);
        }
    }

    public ApplicationMaster(ApplicationMasterConf amConf) {
        this.amConf = amConf;
        yarnConf = new YarnConfiguration();
    }

    public void run() throws IOException, YarnException, InterruptedException {
        LOG.info("Starting ApplicationMaster");

        //AMRMClientAsync.CallbackHandler allocListener = new RMCallbackHandler();
        AMRMClient<ContainerRequest> rmClient = AMRMClient.createAMRMClient();
        rmClient.init(yarnConf);
        rmClient.start();

        //NMClientAsync.CallbackHandler containerListener = new NMCallbackHandler();
        //nmClient = NMClient.createNMClient();//NMClientAsync.createNMClientAsync(new NMCallbackHandler());
        //nmClient.init(yarnConf);
        //nmClient.start();

        // register
        LOG.info("(before registration)");
        rmClient.registerApplicationMaster("", 0, "");
        LOG.info("(after registration)");


        // container request, one for each container ?
        //rmClient.addContainerRequest(createContainerRequest());

        //while (!complete.get()) {
        //    LOG.info("Not complete");
        //    Thread.sleep(100);
        //}
        //rmClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, "", "");
    }

    private ContainerRequest createContainerRequest() {
        // TODO: set params from amConf ?
        return new ContainerRequest(Resource.newInstance(128, 1), null, null, Priority.newInstance(0));
    }

    private ContainerLaunchContext createContainerLaunchContext() {
        Map<String, LocalResource> localResources = new HashMap<>(); // TODO: add TF
        Map<String, String> environment = new HashMap<>(); // TODO: add needed env

        Vector<CharSequence> vargs = new Vector<>();
        vargs.add("python --version");
        vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout");
        vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr");
        StringBuilder command = new StringBuilder();
        for (CharSequence str : vargs) {
            command.append(str).append(" ");
        }
        List<String> commands = Collections.singletonList(command.toString());

        Map<String, ByteBuffer> serviceData = null;
        ByteBuffer tokens = null;
        Map<ApplicationAccessType, String> acls = null;
        return ContainerLaunchContext.newInstance(localResources, environment, commands, serviceData, tokens, acls);
    }

    private class RMCallbackHandler implements AMRMClientAsync.CallbackHandler {
        @Override
        public void onContainersCompleted(List<ContainerStatus> statuses) {
            complete.set(true);
        }

        @Override
        public void onContainersAllocated(List<Container> containers) {
            LOG.info("Got response from RM for container ask, allocatedCnt=" + containers.size());
            for (final Container container : containers) {
                LOG.info("Launching shell command on a new container."
                        + ", containerId=" + container.getId()
                        + ", containerNode=" + container.getNodeId().getHost()
                        + ":" + container.getNodeId().getPort()
                        + ", containerNodeURI=" + container.getNodeHttpAddress()
                        + ", containerResourceMemory"
                        + container.getResource().getMemory()
                        + ", containerResourceVirtualCores"
                        + container.getResource().getVirtualCores());

                Thread launchThread = new Thread(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            nmClient.startContainer/*Async*/(container, createContainerLaunchContext());
                        } catch (YarnException e) {
                            e.printStackTrace();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                });

                launchThreads.add(launchThread);
                launchThread.start();
            }
        }

        @Override
        public void onShutdownRequest() {

        }

        @Override
        public void onNodesUpdated(List<NodeReport> updatedNodes) {

        }

        @Override
        public float getProgress() {
            return 0;
        }

        @Override
        public void onError(Throwable e) {

        }
    }

    /*private class NMCallbackHandler implements NMClientAsync.CallbackHandler {
        @Override
        public void onContainerStarted(ContainerId containerId, Map<String, ByteBuffer> allServiceResponse) {

        }

        @Override
        public void onContainerStatusReceived(ContainerId containerId, ContainerStatus containerStatus) {

        }

        @Override
        public void onContainerStopped(ContainerId containerId) {

        }

        @Override
        public void onStartContainerError(ContainerId containerId, Throwable t) {

        }

        @Override
        public void onGetContainerStatusError(ContainerId containerId, Throwable t) {

        }

        @Override
        public void onStopContainerError(ContainerId containerId, Throwable t) {

        }
    }*/
}
