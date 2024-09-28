import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class BytesMapper extends Mapper<Object, Text, Text, IntWritable> {
    private Text logKey = new Text("BytesTransferred");
    private IntWritable bytesWritable = new IntWritable();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] logEntry = value.toString().split(" ");
        if (logEntry.length > 9) {  // Assuming bytes transferred is at position 9
            try {
                int bytesTransferred = Integer.parseInt(logEntry[9]);
                bytesWritable.set(bytesTransferred);
                context.write(logKey, bytesWritable);
            } catch (NumberFormatException e) {
                // Ignore entries that don't have a valid number for bytes transferred
            }
        }
    }
}

