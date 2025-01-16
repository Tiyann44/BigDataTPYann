package org.epf.hadoop.colfil1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class RelationshipReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Set<String> connections = new HashSet<>();
        for (Text val : values) {
            connections.add(val.toString());
        }

        //Séparation par des virgules
        StringBuilder connectionsList = new StringBuilder();
        for (String connection : connections) {
            if (connectionsList.length() > 0) {
                connectionsList.append(",");
            }
            connectionsList.append(connection);
        }

        context.write(key, new Text(connectionsList.toString()));
    }
}