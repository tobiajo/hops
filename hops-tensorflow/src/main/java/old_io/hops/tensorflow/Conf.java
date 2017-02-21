package old_io.hops.tensorflow;

import org.apache.commons.cli.*;
import org.apache.commons.lang.ArrayUtils;

import java.util.ArrayList;
import java.util.List;

public abstract class Conf {

    Options opts;
    CommandLine cliParser;
    List<String> defaultArgs;

    public Conf(String[] args) throws ParseException {
        opts = new Options();
        defaultArgs = new ArrayList<>();
        cliParser = new GnuParser().parse(opts, (String[]) ArrayUtils.addAll(args, defaultArgs.toArray()));

        if (cliParser.hasOption("help")) {
            printUsage();
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

    void addFlag(String opt, String description) {
        opts.addOption(opt, false, description);
    }

    void addArgument(String opt, String description) {
        opts.addOption(opt, true, description);
    }

    void addArgument(String opt, String description, String defaultValue) {
        opts.addOption(opt, true, description +  ". Default: " + defaultValue);
        defaultArgs.add("--" + opt);
        defaultArgs.add(defaultValue);
    }

    private void printUsage() {
        new HelpFormatter().printHelp("Client", opts);
    }
}
