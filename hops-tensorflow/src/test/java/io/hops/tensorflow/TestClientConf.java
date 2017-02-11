package io.hops.tensorflow;

import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;

import static io.hops.tensorflow.ClientConf.*;
import static org.junit.Assert.assertTrue;

public class TestClientConf {

    private static final Log LOG = LogFactory.getLog(TestClientConf.class);

    @Test
    public void testClientConf() throws ParseException {
        ClientConf clientOptions = new ClientConf(new String[] {});
        assertTrue(clientOptions.get(APP_NAME).equals("hops-tensorflow"));
        assertTrue(clientOptions.getInt(AM_MEMORY) == 512);
        assertTrue(clientOptions.getInt(AM_VCORES) == 2);
        assertTrue(!clientOptions.has(DEBUG));

        clientOptions = new ClientConf(new String[] {
                "--app_name",
                "ArbitrarilyName",
                "--debug"
        });
        assertTrue(clientOptions.get(APP_NAME).equals("ArbitrarilyName"));
        assertTrue(clientOptions.has(DEBUG));
    }
}
