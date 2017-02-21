package old_io.hops.tensorflow;

import org.apache.hadoop.util.JarFinder;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.client.cli.LogsCLI;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class TestClient {
  
  private static final Log LOG = LogFactory.getLog(TestClient.class);
  
  private MiniYARNCluster yarnCluster;
  private YarnConfiguration conf;
  
  protected final static String APP_MASTER_JAR =
      JarFinder.getJar(ApplicationMaster.class);
  
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
    conf.set("yarn.log-aggregation-enable", "true");
    yarnCluster.init(conf);
    yarnCluster.start();
  }
  
  @After
  public void tearDown() throws IOException {
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
    final Client client = new Client(new ClientConf(new String[]{
        "--jar",
        APP_MASTER_JAR,
        "--app_name",
        "ArbitraryName"
    }), new Configuration(yarnCluster.getConfig()));
    
    LOG.info("Submitting application");
    final ApplicationId appId = client.submitApplication();
    ResourceManager rm = yarnCluster.getResourceManager();
    Map<ApplicationId, RMApp> rmApps = rm.getRMContext().getRMApps();
    assertEquals(true, rmApps.containsKey(appId));
    assertEquals("ArbitraryName", rmApps.get(appId).getName());
    
    new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          Thread.sleep(10000);
          client.kill(appId);
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    }).start();
    
    LOG.info("Monitoring application");
    ApplicationReport report = client.monitorApplication(appId, true);
    //assertEquals(YarnApplicationState.KILLED, report.getYarnApplicationState());
    
    Thread.sleep(10000);
    
    LOG.info("Dumping logs");
    LogsCLI logDumper = new LogsCLI();
    logDumper.setConf(conf);
    logDumper.run(new String[]{"-applicationId", appId.toString()});
  }
}
