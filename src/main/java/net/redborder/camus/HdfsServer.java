package net.redborder.camus;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.tools.DistCp;
import org.apache.hadoop.tools.DistCpOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class HdfsServer {
    private static final Logger log = LoggerFactory.getLogger(HdfsServer.class);
    private FileSystem fileSystem = null;
    private final Configuration conf;
    private final String hostname;

    public HdfsServer(String hostname) {
        this.hostname = hostname;
        this.conf = new Configuration();

        try {
            fileSystem = FileSystem.get(new Path("hdfs://" + this.hostname + "/").toUri(), conf);
        } catch (IOException e) {
            log.error("Couldn't connect to HDFS server at {}", hostname);
            System.exit(1);
        }
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
            FileStatus[] items = fileSystem.listStatus(path);
            itemsList = Arrays.asList(items);
        } catch (IOException e) {
            log.error("Couldn't list path {} from server {}", pathStr, hostname);
            log.debug(e.toString());
            itemsList = Collections.emptyList();
        }

        return itemsList;
    }

    public void destroyRecursive(Path path) {
        try {
            fileSystem.delete(path, true);
        } catch (IOException e) {
            log.error("Couldn't delete path {} from server {}", path.toString(), hostname);
            log.debug(e.toString());
        }
    }

    public void distCopy(Path sourceFile, Path destFile) {
        try {
            Configuration conf = new Configuration();
            DistCp distCp = new DistCp(conf, new DistCpOptions(sourceFile, destFile));
            distCp.run(new String[] { sourceFile.toString(), destFile.toString() });
        } catch (Exception e) {
            log.error("Couldn't execute distCp from {} to {}", sourceFile.toString(), destFile.toString());
            e.printStackTrace();
        }
    }
}
