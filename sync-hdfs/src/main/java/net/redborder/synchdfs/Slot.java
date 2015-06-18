package net.redborder.synchdfs;

import com.google.common.base.Joiner;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Slot implements Comparable<Slot> {
    private static final Logger log = LoggerFactory.getLogger(Slot.class);

    private final String camusPath;
    private final HdfsServer server;
    private final String topic;
    private final DateTime time;
    private final List<Path> paths;
    private final String pattern;
    private final String folder;
    private final long events;

    public Slot(String camusPath, HdfsServer server, String topic, DateTime time) {
        this.camusPath = camusPath;
        this.server = server;
        this.topic = topic;
        this.time = time;
        this.paths = loadPaths();
        this.pattern = buildPattern();
        this.folder = buildFolder();
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
        log.info("Uploading file from {} to {}", sourceFile, getFolder());
        String destFileStr = getFolder() + "/" + name;
        server.distCopy(sourceFile, new Path(destFileStr));
    }

    public void upload(Path sourceFile) {
        upload(sourceFile, sourceFile.getName());
    }

    private List<Path> loadPaths() {
        String path = camusPath + "/" + topic + "/11111111/hourly/" + time.getYear() + "/" +
                String.format("%02d", time.getMonthOfYear()) + "/" +
                String.format("%02d", time.getDayOfMonth()) + "/" +
                String.format("%02d", time.getHourOfDay());

        List<FileStatus> fileStatuses = server.listFiles(path);
        List<Path> paths = new ArrayList<>();

        if (fileStatuses.isEmpty()) {
            log.warn("No events in {} at {}, ignoring", path, server.getHostname());
        } else {
            for (FileStatus fileStatus : fileStatuses) {
                paths.add(fileStatus.getPath());
            }
        }

        return paths;
    }

    private String buildPattern() {
        if (paths.isEmpty()) return "";
        Path path = paths.get(0);
        String[] tokens = path.toString().split("/");
        tokens[tokens.length - 1] = "*.gz";
        return Joiner.on('/').join(tokens);
    }

    private String buildFolder() {
        if (paths.isEmpty()) return "";
        Path path = paths.get(0);
        String[] tokens = path.toString().split("/");
        tokens = Arrays.copyOfRange(tokens, 0, tokens.length - 1);
        return Joiner.on('/').join(tokens);
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
