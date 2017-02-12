package io.hops.tensorflow;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;

public class TestClient {

    private static final Log LOG = LogFactory.getLog(TestClient.class);

    private MiniYARNCluster yarnCluster;
    private YarnConfiguration conf;

    @Before
    public void setup() {
        LOG.info("Starting up YARN cluster");
        yarnCluster = new MiniYARNCluster(
                TestClient.class.getSimpleName(),
                1,
                1,
                1,
                1
        );
        conf = new YarnConfiguration();
        yarnCluster.init(conf);
        yarnCluster.start();
    }

    @After
    public void tearDown() {
        if (yarnCluster != null) {
            try {
                yarnCluster.stop();
            } finally {
                yarnCluster = null;
            }
        }
    }

    @Test
    public void testClient() throws Exception {
        LOG.info("Initializing Client");
        Client client = new Client(new ClientConf(new String[] {
                "--app_name",
                "ArbitraryName"
        }), new Configuration(yarnCluster.getConfig()));

        LOG.info("Submitting application");
        ApplicationId appId = client.submitApplication();
        ResourceManager rm = yarnCluster.getResourceManager();
        Map<ApplicationId, RMApp> rmApps = rm.getRMContext().getRMApps();
        assertEquals(true, rmApps.containsKey(appId));
        assertEquals("ArbitraryName", rmApps.get(appId).getName());

        LOG.info("Monitoring application");
        ApplicationReport report = client.monitorApplication(appId, true);
        assertEquals(YarnApplicationState.FAILED, report.getYarnApplicationState());
    }
}
