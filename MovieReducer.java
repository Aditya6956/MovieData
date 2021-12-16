package com.company;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MovieReducer extends Reducer<Text, Text, Text, Text> {
    // 1, <movies.Sabrina,ratings.3.0>

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String name = "";
        String rating = "";

        for(Text t : values){
            String[] s = t.toString().split("\\.");
            if(s[0].equals("movies")){
                name = s[1];
            }else if(s[0].equals("ratings")){
                rating = s[1];
            }
        }
        String str = "THE MOVIE " + name +"HAS RATING OF " + rating;
        context.write(new Text(key), new Text(str));
    }
}
