package net.redborder.pig;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.pig.LoadFunc;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.Map;

public class RbSyncLoader extends LoadFunc {

    private String[] fields;
    private RecordReader reader;
    private TupleFactory tupleFactory;
    private ObjectMapper mapper;

    public RbSyncLoader(String... fields) {
        this.fields = fields;
        tupleFactory = TupleFactory.getInstance();
        mapper = new ObjectMapper();
    }

    @Override
    public void setLocation(String location, org.apache.hadoop.mapreduce.Job job) throws IOException {
        FileInputFormat.setInputPaths(job, location);
    }

    @Override
    public org.apache.hadoop.mapreduce.InputFormat getInputFormat() throws IOException {
        return new TextInputFormat();
    }

    @Override
    public void prepareToRead(org.apache.hadoop.mapreduce.RecordReader recordReader, PigSplit pigSplit) throws IOException {
        this.reader = recordReader;
    }

    @Override
    public Tuple getNext() throws IOException {
        Tuple tuple = null;
        try {
            boolean notDone = reader.nextKeyValue();
            if (!notDone) {
                return null;
            }

            Text value = (Text) reader.getCurrentValue();
            Integer size = fields.length;
            tuple = tupleFactory.newTuple(size + 2);

            if (value != null) {
                Map<String, Object> data = mapper.readValue(value.toString(), Map.class);
                for (Integer i = 0; i < size; i++) {
                    tuple.set(i, data.get(fields[i]));
                }
                tuple.set(size, data);
                tuple.set(size + 1, data.size());
            }

        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return tuple;
    }
}
