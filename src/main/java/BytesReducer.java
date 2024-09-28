import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class BytesReducer extends Reducer<Text, IntWritable, Text, Text> {
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        int count = 0;
        for (IntWritable val : values) {
            sum += val.get();
            count++;
        }
        double average = (count == 0) ? 0 : (double) sum / count;
        context.write(key, new Text("Sum: " + sum + ", Count: " + count + ", Average: " + average));
    }
}

