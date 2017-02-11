package io.hops.tensorflow;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertTrue;

public class TestClient {

    private static final Log LOG = LogFactory.getLog(TestClient.class);

    private MiniYARNCluster yarnCluster;
    private YarnConfiguration conf;

    @Before
    public void setup() {
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
        Client client = new Client(new ClientConf(new String[] {
                "--app_name",
                "ArbitrarilyName"
        }), new Configuration(yarnCluster.getConfig()));
        ApplicationId appId = client.submitApplication();

        ResourceManager rm = yarnCluster.getResourceManager();
        Map<ApplicationId, RMApp> rmApps = rm.getRMContext().getRMApps();
        assertTrue(rmApps.containsKey(appId));
        String appName = rmApps.get(appId).getName();
        assertTrue(appName.equals("ArbitrarilyName"));
    }
}
