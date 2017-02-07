package io.hops.tensorflow;

import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.exceptions.YarnException;

import java.io.IOException;

public class Client {

    private static final Log LOG = LogFactory.getLog(Client.class);

    // Command line options
    private Options opts;

    public static void main(String[] args) {
        boolean result = false;
        try {
            Client client = new Client();
            LOG.info("Initializing Client");
            try {
                boolean doRun = client.init(args);
                if (!doRun) {
                    System.exit(0);
                }
            } catch (IllegalArgumentException e) {
                System.err.println(e.getLocalizedMessage());
                client.printUsage();
                System.exit(-1);
            }
            result = client.run();
        } catch (Throwable t) {
            LOG.fatal("Error running Client", t);
            System.exit(1);
        }
        if (result) {
            LOG.info("Application completed successfully");
            System.exit(0);
        }
        LOG.error("Application failed to complete successfully");
        System.exit(2);
    }

    public boolean init(String[] args) throws ParseException {
        return true;
    }

    public boolean run() throws IOException, YarnException {
        return true;
    }

    private void printUsage() {
        new HelpFormatter().printHelp("Client", opts);
    }
}
