package io.hops.tensorflow;

import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;

import static io.hops.tensorflow.ClientConf.*;
import static org.junit.Assert.assertEquals;

public class TestClientConf {

    private static final Log LOG = LogFactory.getLog(TestClientConf.class);

    @Test
    public void testClientConf() throws Exception {
        LOG.info("Instantiating ClientConf without args");
        ClientConf clientOptions = new ClientConf(new String[] {});
        assertEquals("hops-tensorflow", clientOptions.get(APP_NAME));
        assertEquals(512, clientOptions.getInt(AM_MEMORY));
        assertEquals(2, clientOptions.getInt(AM_VCORES));
        assertEquals(false, clientOptions.has(DEBUG));

        LOG.info("Instantiating ClientConf with some args");
        clientOptions = new ClientConf(new String[] {
                "--app_name",
                "ArbitraryName",
                "--debug"
        });
        assertEquals("ArbitraryName", clientOptions.get(APP_NAME));
        assertEquals(true, clientOptions.has(DEBUG));
    }
}
