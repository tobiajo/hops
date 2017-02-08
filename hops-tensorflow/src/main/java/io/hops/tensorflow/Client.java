package io.hops.tensorflow;

import org.apache.commons.cli.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;

import java.io.IOException;

public class Client {

    private static final Log LOG = LogFactory.getLog(Client.class);

    // Configuration
    private Configuration conf;
    private YarnClient yarnClient;

    // Application master jar file
    private String appMasterJar = "";

    // Debug flag
    boolean debugFlag = false;

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

    public Client() throws Exception {
        this.conf = new YarnConfiguration();
        yarnClient = YarnClient.createYarnClient();
        yarnClient.init(conf);
        opts = new Options();
        opts.addOption("jar", true, "Jar file containing the application master");
        opts.addOption("debug", false, "Dump out debug information");
        opts.addOption("help", false, "Print usage");
    }

    public boolean init(String[] args) throws ParseException {
        CommandLine cliParser = new GnuParser().parse(opts, args);

        if (args.length == 0) {
            throw new IllegalArgumentException("No args specified for client to initialize");
        }

        if (cliParser.hasOption("help")) {
            printUsage();
            return false;
        }

        if (cliParser.hasOption("debug")) {
            debugFlag = true;

        }

        if (!cliParser.hasOption("jar")) {
            throw new IllegalArgumentException("No jar file specified for application master");
        }

        appMasterJar = cliParser.getOptionValue("jar");

        return true;
    }

    public boolean run() throws IOException, YarnException {
        LOG.info("Running Client");
        yarnClient.start();

        // Get a new application id
        YarnClientApplication app = yarnClient.createApplication();
        GetNewApplicationResponse appResponse = app.getNewApplicationResponse();

        // set the application submission context
        ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();
        ApplicationId appId = appContext.getApplicationId();

        // TODO: Investigate what AppContext setters that is needed

        /*
        appContext.setKeepContainersAcrossApplicationAttempts(keepContainers);
        appContext.setApplicationName(appName);
        appContext.setNodeLabelExpression(nodeLabelExpression);
        appContext.setResource(capability);
        appContext.setAMContainerSpec(amContainer);
        appContext.setPriority(pri);
        appContext.setQueue(amQueue);
        */

        LOG.info("Submitting application to ASM");
        yarnClient.submitApplication(appContext);

        // TODO: Add monitoring

        return true;
    }

    private void printUsage() {
        new HelpFormatter().printHelp("Client", opts);
    }
}
