package net.redborder.camus;

import org.apache.commons.cli.*;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Arrays;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;

public class CamusSync {
    public static Logger log = LoggerFactory.getLogger(CamusSync.class);

    public static void main(String[] args) {
        Options options = new Options();
        options.addOption("m", "mode", true, "task to execute (synchronize, deduplicate)");
        options.addOption("w", "window", true, "window hours");
        options.addOption("f", "offset", true, "offset");
        options.addOption("n", "namenodes", true, "comma separated list of namenodes");
        options.addOption("t", "topics", true, "comma separated list of topics");
        options.addOption("s", "namespaces", true, "comma separated list of namespaces");
        options.addOption("d", "dimensions-file", true, "path to a YAML file that specifies" +
                " an array of dimensions for each topic that will be used to identify duplicated events");
        options.addOption("c", "camus-path", true, "HDFS path where camus saves its data");
        options.addOption("N", "dry-run", false, "do nothing");
        options.addOption("h", "help", false, "print this help");

        CommandLine cmdLine = null;
        CommandLineParser parser = new BasicParser();
        HelpFormatter helpFormatter = new HelpFormatter();

        try {
            cmdLine = parser.parse(options, args);
        } catch (ParseException e) {
            log.error("Couldn't parse options. Use the options below.");
        }

        if (cmdLine == null || cmdLine.hasOption("h")) {
            helpFormatter.printHelp("java -cp CLASSPATH " + CamusSync.class.getCanonicalName() + " OPTIONS", options);
            System.exit(1);
        }

        boolean dryrun = cmdLine.hasOption("N");
        String camusPath = cmdLine.getOptionValue("c");
        String namenodesList = cmdLine.getOptionValue("n");
        String[] namenodes = namenodesList.split(",");

        int offsetHours = Integer.valueOf(cmdLine.getOptionValue("f"));
        int windowHours = Integer.valueOf(cmdLine.getOptionValue("w"));
        DateTime currentHour = DateTime.now().withMinuteOfHour(0);
        DateTime offsetHour = currentHour.minusHours(offsetHours);
        Interval interval = new Interval(offsetHour.minusHours(windowHours), offsetHour);
        String mode = cmdLine.getOptionValue("m");

        DimensionsFile dimensionsFile = null;
        if (cmdLine.hasOption("d")) {
            try {
                String dimensionsFilePath = cmdLine.getOptionValue("d");
                dimensionsFile = new DimensionsFile(dimensionsFilePath);
            } catch (FileNotFoundException e) {
                log.error("Couldn't find the dimensions file. Please check the path and try again");
                System.exit(1);
            }
        } else {
            log.error("You must specify the dimensions file to run the deduplicate job");
            System.exit(1);
        }

        List<String> topics = null;
        if (cmdLine.hasOption("t")) {
            String topicsList = cmdLine.getOptionValue("t");
            topics = Arrays.asList(topicsList.split(","));
        } else {
            topics = new ArrayList<>(dimensionsFile.getTopics());
        }

        Hdfs hdfs = new Hdfs(camusPath, namenodes);

        for (String topic: topics) {
            List<String> namespaces;
            if (cmdLine.hasOption("s")) {
                String namespacesList = cmdLine.getOptionValue("s");
                namespaces = Arrays.asList(namespacesList.split(","));
            } else {
                namespaces = hdfs.namespaces(camusPath, topic);
            }

            for (String namespace: namespaces) {
                List<SlotOptions> slotOptionsList = hdfs.slotsOptions(topic, namespace, interval);

                for (SlotOptions slotOptions : slotOptionsList) {
                    if (mode.equals("deduplicate")) {
                        if (cmdLine.hasOption("d")) {
                            List<String> dimensionsList = dimensionsFile.getDimensionsFromTopic(topic);
                            slotOptions.deduplicate(dryrun, dimensionsList);
                        } else {
                            log.error("You must specify at least one dimension to run the deduplicate job");
                        }
                    } else if (mode.equals("synchronize")) {
                        slotOptions.synchronize(dryrun);
                    }
                }
            }
        }
    }
}
