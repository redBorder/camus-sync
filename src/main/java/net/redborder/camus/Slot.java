package net.redborder.camus;

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
    private final String fullFolder;
    private final String folder;
    private final long randomNumber;
    private final long events;

    public Slot(String camusPath, HdfsServer server, String topic, DateTime time) {
        this.server = server;
        this.topic = topic;
        this.time = time;

        this.folder = camusPath + "/" + topic + "/hourly/" + time.getYear() + "/" +
                String.format("%02d", time.getMonthOfYear()) + "/" +
                String.format("%02d", time.getDayOfMonth()) + "/" +
                String.format("%02d", time.getHourOfDay());

        this.fullFolder = "hdfs://" + server.getHostname() + this.folder;
        this.pattern = this.folder + "/*.gz";
        this.paths = loadPaths();
        this.events = getEventsFromFileName();
        this.randomNumber = getRandomNumberFromFileName();
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

    public String getFullFolder() {
        return fullFolder;
    }

    public long getRandomNumber() {
        return randomNumber;
    }

    public void destroy() {
        log.info("Deleting data from slot with topic {} time {}", topic, time);

        for (Path path : paths) {
            server.destroyRecursive(path);
        }
    }

    public void upload(Path sourceFile, String name) {
        String destFileStr = getFullFolder() + "/" + name;
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
            log.debug("No events in {} at {}, ignoring", folder, server.getHostname());
        } else {
            for (FileStatus fileStatus : fileStatuses) {
                paths.add(fileStatus.getPath());
            }
        }

        return paths;
    }

    private long getEventsFromFileName() {
        long events = 0;

        for (Path path : paths) {
            String fileName = path.getName();
            String[] tokens = fileName.split("\\.");
            events += Long.parseLong(tokens[3]);
        }

        return events;
    }

    private long getRandomNumberFromFileName() {
        long events = 0;

        for (Path path : paths) {
            String fileName = path.getName();
            String[] tokens = fileName.split("\\.");
            String numberStr = tokens[4];

            // TODO: This if block is to avoid an error if the user used camus-sync prior to version 0.2.1
            // We should remove this if in the future, when we stop using clusters with camus-sync 0.2.0- data.
            if (!numberStr.equals("without")) {
                events += Long.parseLong(tokens[4]);
            }
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
