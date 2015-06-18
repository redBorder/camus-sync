package net.redborder.synchdfs;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class Slot implements Comparable<Slot> {
    private static final Logger log = LoggerFactory.getLogger(Slot.class);

    private final HdfsServer server;
    private final String topic;
    private final DateTime time;
    private final List<Path> paths;
    private final String pattern;
    private final String folder;
    private final long events;

    public Slot(String camusPath, HdfsServer server, String topic, DateTime time) {
        this.server = server;
        this.topic = topic;
        this.time = time;

        this.folder = camusPath + "/" + topic + "/hourly/" + time.getYear() + "/" +
                String.format("%02d", time.getMonthOfYear()) + "/" +
                String.format("%02d", time.getDayOfMonth()) + "/" +
                String.format("%02d", time.getHourOfDay());


        this.pattern = this.folder + "/*.gz";
        this.paths = loadPaths();
        this.events = computeEvents();
    }

    public HdfsServer getServer() {
        return server;
    }

    public String getTopic() {
        return topic;
    }

    public DateTime getTime() {
        return time;
    }

    public List<Path> getPaths() {
        return paths;
    }

    public String getPattern() {
        return pattern;
    }

    public long getEvents() {
        return events;
    }

    public String getFolder() {
        return folder;
    }

    public void destroy() {
        log.info("Deleting data from slot with topic {} time {}", topic, time);

        for (Path path : paths) {
            server.destroyRecursive(path);
        }
    }

    public void upload(Path sourceFile, String name) {
        String destFileStr = "hdfs://" + server.getHostname() + getFolder() + "/" + name;
        log.info("Uploading file from {} to {}", sourceFile, destFileStr);
        server.distCopy(sourceFile, new Path(destFileStr));
    }

    public void upload(Path sourceFile) {
        upload(sourceFile, sourceFile.getName());
    }

    private List<Path> loadPaths() {
        List<FileStatus> fileStatuses = server.listFiles(folder);
        List<Path> paths = new ArrayList<>();

        if (fileStatuses.isEmpty()) {
            log.warn("No events in {} at {}, ignoring", folder, server.getHostname());
        } else {
            for (FileStatus fileStatus : fileStatuses) {
                paths.add(fileStatus.getPath());
            }
        }

        return paths;
    }

    private long computeEvents() {
        long events = 0;

        for (Path path : paths) {
            String fileName = path.getName();
            String[] tokens = fileName.split("\\.");
            events += Long.parseLong(tokens[3]);
        }

        return events;
    }

    @Override
    public int compareTo(Slot o) {
        if (events > o.events) {
            return 1;
        } else if (events < o.events){
            return -1;
        } else {
            return 0;
        }
    }
}
