package old_io.hops.tensorflow;

import org.apache.commons.cli.*;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.List;

public class ClientConf {
  
  public static final String APP_NAME = "app_name";
  public static final String AM_MEMORY = "am_memory";
  public static final String AM_VCORES = "am_vcores";
  public static final String QUEUE_NAME = "queue_name";
  public static final String REPORT_INTERVAL = "report_interval";
  public static final String JAR = "jar";
  public static final String DEBUG = "debug";
  public static final String HELP = "help";
  
  private static final Log LOG = LogFactory.getLog(ClientConf.class);
  
  private Options opts;
  private CommandLine cliParser;
  private List<String> defaultArgs;
  
  {
    opts = new Options();
    defaultArgs = new ArrayList<>();
    addArgument(APP_NAME, "Application Name", "hops-tensorflow");
    addArgument(AM_MEMORY,
        "Amount of memory in MB to be requested to run the application master",
        "512");
    addArgument(AM_VCORES,
        "Amount of virtual cores to be requested to run the application master",
        "2");
    addArgument(QUEUE_NAME,
        "RM Queue in which this application is to be submitted", "default");
    addArgument(REPORT_INTERVAL,
        "Interval in ms for checking application status", "1000");
    addArgument(JAR, "Jar file containing the application master");
    addFlag(DEBUG, "Dump out debug information");
    addFlag(HELP, "Print usage");
  }
  
  public ClientConf(String[] args) throws ParseException {
    LOG.info("Initializing ClientConf");
    cliParser = new GnuParser()
        .parse(opts, (String[]) ArrayUtils.addAll(args, defaultArgs.toArray()));
    
    if (cliParser.hasOption("help")) {
      printUsage();
    }
    
    if (!cliParser.hasOption("jar")) {
      throw new IllegalArgumentException(
          "No jar file specified for application master");
    }
  }
  
  public boolean has(String opt) {
    return cliParser.hasOption(opt);
  }
  
  public String get(String opt) {
    return cliParser.getOptionValue(opt);
  }
  
  public int getInt(String opt) {
    return Integer.parseInt(get(opt));
  }
  
  private void addFlag(String opt, String description) {
    opts.addOption(opt, false, description);
  }
  
  private void addArgument(String opt, String description) {
    opts.addOption(opt, true, description);
  }
  
  private void addArgument(String opt, String description,
      String defaultValue) {
    opts.addOption(opt, true, description + ". Default: " + defaultValue);
    defaultArgs.add("--" + opt);
    defaultArgs.add(defaultValue);
  }
  
  private void printUsage() {
    new HelpFormatter().printHelp("Client", opts);
  }
}
