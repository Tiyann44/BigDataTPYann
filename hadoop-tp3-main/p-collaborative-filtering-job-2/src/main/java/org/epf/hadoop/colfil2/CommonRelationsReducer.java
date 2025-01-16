package org.epf.hadoop.colfil2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;

public class CommonRelationsReducer extends Reducer<UserPair, Text, UserPair, Text> {

    @Override
    protected void reduce(UserPair key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        HashSet<String> sources = new HashSet<>();
        boolean isDirect = false;

        for (Text value : values) {
            if (value.toString().equals("DIRECT")) {
                isDirect = true;
            } else {
                sources.add(value.toString());
            }
        }

        if (!isDirect && sources.size() > 1) {
            context.write(key, new Text(String.valueOf(sources.size())));
        }
    }
}
