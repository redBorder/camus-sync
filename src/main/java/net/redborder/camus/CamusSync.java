package net.redborder.camus;

import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class CamusSync {
    public static Logger log = LoggerFactory.getLogger(CamusSync.class);

    public static void main(String[] args) {
        OptionsWithFile options = new OptionsWithFile(args, "configFile");
        options.addOption("m", "mode", true, "task to execute (synchronize, deduplicate)");
        options.addOption("w", "window", true, "window hours");
        options.addOption("f", "offset", true, "offset");
        options.addOption("n", "namenodes", true, "comma separated list of namenodes");
        options.addOption("t", "topics", true, "comma separated list of topics");
        options.addOption("c", "configFile", true, "path to a YAML config file");
        options.addOption("p", "camusPath", true, "HDFS path where camus saves its data");
        options.addOption("d", "dryRun", false, "do nothing");
        options.addOption("h", "help", false, "print this help");
        options.build();

        if (options.hasOption("help")) {
            options.printHelp("java -cp CLASSPATH " + CamusSync.class.getCanonicalName() + " OPTIONS");
            System.exit(1);
        }

        boolean dryrun = options.hasOption("dryRun");
        String camusPath = options.getOption("camusPath");
        List<String> namenodes = options.getListOption("namenodes");

        int offsetHours = Integer.valueOf(options.getOption("offset"));
        int windowHours = Integer.valueOf(options.getOption("window"));
        DateTime currentHour = DateTime.now().withMinuteOfHour(0);
        DateTime offsetHour = currentHour.minusHours(offsetHours);
        Interval interval = new Interval(offsetHour.minusHours(windowHours), offsetHour);
        String mode = options.getOption("mode");

        List<String> topics = options.getListKeysOption("topics");
        topics.remove("default");
        if (topics.isEmpty()) {
            log.error("No topic specified. Check the options and try again.");
            System.exit(1);
        }

        Hdfs hdfs = new Hdfs(camusPath, namenodes);

        for (String topic: topics) {
            List<SlotOptions> slotOptionsList = hdfs.slotsOptions(topic, interval);

            if (slotOptionsList.isEmpty()) {
                log.warn("No slot options for topic {}", topic);
                continue;
            }

            for (SlotOptions slotOptions : slotOptionsList) {
                if (mode.equals("deduplicate")) {
                    List<String> dimensionsList = options.getConfig().getDimensionsFromTopic(topic);
                    if (dimensionsList.isEmpty()) {
                        log.error("You must specify at least one dimension to run the deduplicate job");
                        continue;
                    }

                    slotOptions.deduplicate(dryrun, dimensionsList);
                } else if (mode.equals("synchronize")) {
                    slotOptions.synchronize(dryrun);
                } else {
                    log.error("Mode not specified or not available");
                    log.error("Available modes are deduplicate and synchronize");
                    log.error("Exiting...");
                    System.exit(1);
                }
            }
        }

        hdfs.cleanup();
    }
}
