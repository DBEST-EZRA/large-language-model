import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;

public class IPReducer extends Reducer<Text, Text, Text, Text> {
    private HashSet<String> uniqueIPs = new HashSet<>();

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // Add IP addresses to the set to ensure uniqueness
        uniqueIPs.add(key.toString());
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // Output the total count of unique IPs
        context.write(new Text("Unique IPs"), new Text(String.valueOf(uniqueIPs.size())));
    }
}

