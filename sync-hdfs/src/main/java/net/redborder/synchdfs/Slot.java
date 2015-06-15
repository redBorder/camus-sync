package net.redborder.synchdfs;

import com.google.common.base.Joiner;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class Slot implements Comparable<Slot> {
    private static final Logger log = LoggerFactory.getLogger(Slot.class);

    private final String camusPathStr;
    private final HdfsServer server;
    public final String topic;
    public final DateTime time;
    public final List<Path> paths;
    public final String pattern;
    public final long events;

    public Slot(String camusPathStr, HdfsServer server, String topic, DateTime time) {
        this.camusPathStr = camusPathStr;
        this.server = server;
        this.topic = topic;
        this.time = time;
        this.paths = getPaths();
        this.pattern = getPattern();
        this.events = getEvents();
    }

    public List<Path> getPaths() {
        String path = camusPathStr + "/" + topic + "/11111111/hourly/" + time.getYear() + "/" +
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

    public String getPattern() {
        if (paths.isEmpty()) return "";
        Path path = paths.get(0);
        String[] tokens = path.toString().split("/");
        tokens[tokens.length - 1] = "*.gz";
        return Joiner.on('/').join(tokens);
    }

    public long getEvents() {
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
