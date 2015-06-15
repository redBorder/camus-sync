package net.redborder.synchdfs;

import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class Hdfs {
    private static final Logger log = LoggerFactory.getLogger(Hdfs.class);
    private List<HdfsServer> servers;
    private String camusPathStr;

    public Hdfs(String camusPathStr, String[] namenodes) {
        this.servers = new ArrayList<>();
        this.camusPathStr = camusPathStr;

        for (String namenode : namenodes) {
            HdfsServer server = new HdfsServer(namenode);
            if (!server.listFiles("/").isEmpty()) servers.add(server);
        }
    }

    public List<SlotOptions> slotsOptions(String topic, Interval interval) {
        List<SlotOptions> slotOptions = new ArrayList<>();
        DateTime start = interval.getStart().withMinuteOfHour(0);
        log.info("scanning HDFS options for topic {} from {} to {}", topic, interval.getStart(), interval.getEnd());

        do {
            slotOptions.add(new SlotOptions(camusPathStr, servers, topic, start));
            start = start.plusHours(1);
        } while (interval.contains(start));

        return slotOptions;
    }
}
