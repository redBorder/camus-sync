package net.redborder.synchdfs;

import org.apache.commons.cli.*;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class SyncHdfs {
    public static Logger log = LoggerFactory.getLogger(SyncHdfs.class);

    public static void main(String[] args) {
        Options options = new Options();
        options.addOption("w", "window", true, "window hours");
        options.addOption("f", "offset", true, "offset");
        options.addOption("n", "namenodes", true, "comma separated list of namenodes");
        options.addOption("t", "topics", true, "comma separated list of topics");
        options.addOption("c", "camus-path", true, "HDFS path where camus saves its data");
        options.addOption("h", "help", false, "print this help");
        options.addOption("N", "dry-run", false, "do nothing");

        CommandLine cmdLine = null;
        CommandLineParser parser = new BasicParser();
        HelpFormatter helpFormatter = new HelpFormatter();

        try {
            cmdLine = parser.parse(options, args);
        } catch (ParseException e) {
            log.error("Couldn't parse options. Use the options below.");
        }

        if (cmdLine == null || cmdLine.hasOption("h")) {
            helpFormatter.printHelp(SyncHdfs.class.getCanonicalName(), options);
            System.exit(1);
        }

        String camusPath = cmdLine.getOptionValue("c");
        String namenodesList = cmdLine.getOptionValue("n");
        String[] namenodes = namenodesList.split(",");
        Hdfs hdfs = new Hdfs(camusPath, namenodes);

        int offsetHours = Integer.valueOf(cmdLine.getOptionValue("f"));
        int windowHours = Integer.valueOf(cmdLine.getOptionValue("w"));
        DateTime currentHour = DateTime.now().withMinuteOfHour(0);
        DateTime offsetHour = currentHour.minusHours(offsetHours);
        Interval interval = new Interval(offsetHour.minusHours(windowHours), offsetHour);

        String topicsList = cmdLine.getOptionValue("t");
        String[] topics = topicsList.split(",");

        for (String topic: topics) {
            List<SlotOptions> slotOptionsList = hdfs.slotsOptions(topic, interval);

            for (SlotOptions slotOptions : slotOptionsList) {
                slotOptions.deduplicate();
            }
        }
    }
}