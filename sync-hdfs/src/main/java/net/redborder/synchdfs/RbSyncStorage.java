package net.redborder.synchdfs;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.pig.StoreFunc;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigOutputFormat;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigTextOutputFormat;
import org.apache.pig.data.Tuple;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.Map;

public class RbSyncStorage extends StoreFunc {

    private RecordWriter writer;
    private ObjectMapper mapper;

    public RbSyncStorage() {
        mapper = new ObjectMapper();
    }

    @Override
    public OutputFormat getOutputFormat() throws IOException {
        return new TextOutputFormat();
    }

    @Override
    public void setStoreLocation(String location, Job job) throws IOException {
        FileOutputFormat.setOutputPath(job, new Path(location));

        if ("true".equals(job.getConfiguration().get("output.compression.enabled"))) {
            FileOutputFormat.setCompressOutput(job, true);
            String codec = job.getConfiguration().get("output.compression.codec");
            try {
                FileOutputFormat.setOutputCompressorClass(job, (Class<? extends CompressionCodec>) Class.forName(codec));
            } catch (ClassNotFoundException e) {
                throw new RuntimeException("Class not found: " + codec);
            }
        } else {
            // This makes it so that storing to a directory ending with ".gz" or ".bz2" works.
            setCompression(new Path(location), job);
        }
    }

    private void setCompression(Path path, Job job) {
        String location = path.getName();
        if (location.endsWith(".gz")) {
            FileOutputFormat.setCompressOutput(job, true);
            FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
        } else {
            FileOutputFormat.setCompressOutput(job, false);
        }
    }

    @Override
    public void prepareToWrite(RecordWriter recordWriter) throws IOException {
        this.writer = recordWriter;
    }

    @Override
    public void putNext(Tuple tuple) throws IOException {
        Map<String, Object> data = (Map<String, Object>) tuple.get(0);

        String json = mapper.writeValueAsString(data);

        try {
            writer.write(null, json);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
