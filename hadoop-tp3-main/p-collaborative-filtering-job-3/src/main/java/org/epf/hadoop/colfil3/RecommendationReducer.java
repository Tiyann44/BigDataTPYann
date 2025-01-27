package org.epf.hadoop.colfil3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class RecommendationReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        List<String> recommendations = new ArrayList<>();


        for (Text value : values) {
            recommendations.add(value.toString());
        }


        recommendations.sort(new Comparator<String>() {
            @Override
            public int compare(String o1, String o2) {
                int count1 = Integer.parseInt(o1.split(":")[1]);
                int count2 = Integer.parseInt(o2.split(":")[1]);
                return Integer.compare(count2, count1);
            }
        });


        List<String> top5Recommendations = recommendations.subList(0, Math.min(5, recommendations.size()));


        String result = String.join(",", top5Recommendations);

        context.write(key, new Text(result));
    }
}
