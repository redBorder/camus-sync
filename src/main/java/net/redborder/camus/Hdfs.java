package net.redborder.camus;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
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

    public Hdfs(String camusPathStr, List<String> namenodes) {
        this.servers = new ArrayList<>();
        this.camusPathStr = camusPathStr;

        for (String namenode : namenodes) {
            HdfsServer server = new HdfsServer(namenode);
            if (!server.listFiles("/").isEmpty()) servers.add(server);
        }
    }

    public List<String> namespaces(String camusPath, String topic) {
        HdfsServer server = servers.get(0);
        List<FileStatus> fileStatuses = server.listFiles(camusPath + "/" + topic);
        List<String> namespaces = new ArrayList<>();

        for (FileStatus fileStatus : fileStatuses) {
            String fileName = fileStatus.getPath().getName();

            if (fileName.matches("\\A[0-9]*\\Z") || fileName.equals("default")) {
                namespaces.add(fileName);
            }
        }

        log.info("Detected namespaces for topic {} are {}", topic, namespaces);
        return namespaces;
    }

    public List<SlotOptions> slotsOptions(String topic, String namespace, Interval interval) {
        List<SlotOptions> slotOptions = new ArrayList<>();
        DateTime start = interval.getStart().withMinuteOfHour(0);
        log.info("scanning HDFS options for topic {} namespace {} from {} to {}", topic, namespace, interval.getStart(), interval.getEnd());

        do {
            slotOptions.add(new SlotOptions(camusPathStr, servers, topic, namespace, start));
            start = start.plusHours(1);
        } while (interval.contains(start));

        return slotOptions;
    }

    public void cleanup() {
        for (HdfsServer server : servers) {
            server.destroyRecursive(new Path("/camus-sync"));
        }
    }
}
