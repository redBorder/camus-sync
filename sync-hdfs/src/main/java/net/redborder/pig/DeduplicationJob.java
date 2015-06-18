package net.redborder.pig;

import com.google.common.base.Joiner;
import org.apache.hadoop.fs.Path;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.executionengine.ExecJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

public class DeduplicationJob {
    private static final Logger log = LoggerFactory.getLogger(DeduplicationJob.class);
    private List<String> files;
    private PigServer pigServer;

    public DeduplicationJob(List<String> files) {
        this.files = files;

        Properties props = new Properties();
        props.setProperty("output.compression.enabled", "true");
        props.setProperty("output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");

        try {
            this.pigServer = new PigServer(ExecType.MAPREDUCE, props);
        } catch (ExecException e) {
            log.error("Couldn't execute pig server: {}", e.getMessage());
            e.printStackTrace();
        }
    }

    public Results run() {
        String joinedFiles = Joiner.on(',').join(files);
        String uuid = UUID.randomUUID().toString();
        String outputFolder = "/camus-sync/" + uuid + "/data";
        Results results;

        try {
            pigServer.registerQuery("RAW_DATA = LOAD '" + joinedFiles + "'" +
                                    "USING net.redborder.pig.RbSyncLoader('timestamp','src','dst')" +
                                    "AS (timestamp:chararray, src:chararray, dst:chararray, data:Map[], count:int);");
            pigServer.registerQuery("GROUP_DATA = GROUP RAW_DATA BY (timestamp, src, dst);");
            pigServer.registerQuery("DEDUPLICATE_DATA = FOREACH GROUP_DATA {" +
                                    "    ORDER_DATA = ORDER RAW_DATA BY count DESC;" +
                                    "      JSON_DATA = LIMIT ORDER_DATA 1;" +
                                    "      GENERATE FLATTEN(JSON_DATA.data) AS (raw:Map[]);" +
                                    "};");

            ExecJob execJob = pigServer.store("DEDUPLICATE_DATA", outputFolder, "net.redborder.pig.RbSyncStorage");

            String serverAddr = pigServer.getPigContext().getProperties().getProperty("fs.defaultFS");
            String[] serverTokens = serverAddr.split(":");
            serverTokens = Arrays.copyOfRange(serverTokens, 0, 2);
            String serverAddrWithoutPort = Joiner.on(':').join(serverTokens);
            String fullPath = serverAddrWithoutPort + outputFolder;

            results = new Results(execJob.getStatistics().getNumberRecords(outputFolder), fullPath, uuid);
        } catch (IOException e) {
            log.error("Couldn't execute pig query for jobId {} and files {}", uuid, files);
            e.printStackTrace();
            results = new Results(0, outputFolder, uuid);
        }

        return results;
    }

    public class Results {
        private final long numberRecords;
        private final String folder;
        private final String fileName = "part-r-00000.gz";
        private final Path path;
        private final String uuid;

        public Results(long numberRecords, String folder, String uuid) {
            this.numberRecords = numberRecords;
            this.folder = folder;
            this.path = new Path(folder + "/" + fileName);
            this.uuid = uuid;
        }

        public long getNumberRecords() {
            return numberRecords;
        }

        public String getFolder() {
            return folder;
        }

        public String getFileName() {
            return fileName;
        }

        public Path getPath() {
            return path;
        }

        public String getUuid() {
            return uuid;
        }
    }
}
