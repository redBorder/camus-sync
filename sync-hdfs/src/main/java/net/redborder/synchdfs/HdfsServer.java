package net.redborder.synchdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class HdfsServer {
    private static final Logger log = LoggerFactory.getLogger(HdfsServer.class);
    private final Configuration conf;
    private final String hostname;

    public HdfsServer(String hostname) {
        this.hostname = hostname;
        this.conf = new Configuration();
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
    }

    public String getHostname() {
        return hostname;
    }

    public Path buildPath(String path) {
        StringBuilder pathBuilder = new StringBuilder();
        pathBuilder.append("hdfs://");
        pathBuilder.append(this.hostname);
        pathBuilder.append(path);
        return new Path(pathBuilder.toString());
    }

    public List<FileStatus> listFiles(String pathStr) {
        Path path = buildPath(pathStr);
        List<FileStatus> itemsList;

        try {
            FileSystem fileSystem = FileSystem.get(path.toUri(), conf);
            FileStatus[] items = fileSystem.listStatus(path);
            itemsList = Arrays.asList(items);
        } catch (IOException e) {
            log.error("Couldn't list path {} from server {}", pathStr, hostname);
            itemsList = Collections.emptyList();
        }

        return itemsList;
    }
}
