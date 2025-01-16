package org.epf.hadoop.colfil3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class RecommendationMapper extends Mapper<Object, Text, Text, Text> {

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // Input: user1,user2    count
        String[] line = value.toString().split("\t");
        if (line.length != 2) {
            return; // Ignore malformed lines
        }

        String[] users = line[0].split(",");
        String count = line[1];

        if (users.length == 2) {
            String user1 = users[0];
            String user2 = users[1];

            // Emit recommendations for both users
            context.write(new Text(user1), new Text(user2 + ":" + count));
            context.write(new Text(user2), new Text(user1 + ":" + count));
        }
    }
}
