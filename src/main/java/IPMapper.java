import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class IPMapper extends Mapper<Object, Text, Text, Text> {
    private Text ipAddress = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] logEntry = value.toString().split(" ");
        if (logEntry.length > 0) {
            ipAddress.set(logEntry[0]); // Assuming the IP address is in the first position
            context.write(ipAddress, new Text("1")); // Send 1 as a placeholder
        }
    }
}

