package org.epf.hadoop.colfil2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CommonRelationsMapper extends Mapper<Object, Text, UserPair, Text> {

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        String[] line = value.toString().split("\t");
        if (line.length != 2) {
            return;
        }

        String user = line[0];
        String[] relations = line[1].split(",");


        for (int i = 0; i < relations.length; i++) {
            for (int j = i + 1; j < relations.length; j++) {
                UserPair pair = new UserPair(relations[i], relations[j]);
                context.write(pair, new Text(user));
            }
        }


        for (String relation : relations) {
            UserPair directPair = new UserPair(user, relation);
            context.write(directPair, new Text("DIRECT"));
        }
    }
}
