import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class LogTypeMapper extends Mapper<Object, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text logType = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] logEntry = value.toString().split(" ");
        if (logEntry.length > 2) {
            logType.set(logEntry[2]); // Assuming log type is in the 3rd position
            context.write(logType, one);
        }
    }
}
