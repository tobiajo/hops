package old_io.hops.tensorflow;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestClientConf {
  
  private static final Log LOG = LogFactory.getLog(TestClientConf.class);
  
  @Test
  public void testClientConf() throws Exception {
    LOG.info("Instantiating ClientConf without args");
    ClientConf clientOptions =
        new ClientConf(new String[]{"--jar", "dummy.jar"});
    assertEquals("hops-tensorflow", clientOptions.get(ClientConf.APP_NAME));
    assertEquals(512, clientOptions.getInt(ClientConf.AM_MEMORY));
    assertEquals(2, clientOptions.getInt(ClientConf.AM_VCORES));
    assertEquals(false, clientOptions.has(ClientConf.DEBUG));
    
    LOG.info("Instantiating ClientConf with some args");
    clientOptions = new ClientConf(new String[]{
        "--jar",
        "dummy.jar",
        "--app_name",
        "ArbitraryName",
        "--debug"
    });
    assertEquals("ArbitraryName", clientOptions.get(ClientConf.APP_NAME));
    assertEquals(true, clientOptions.has(ClientConf.DEBUG));
  }
}
