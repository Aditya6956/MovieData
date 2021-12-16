package com.company;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class RatingMapper extends Mapper<LongWritable, Text, Text, Text> {
    //userId,movieId,rating,timestamp
    //1,2,3.5,1112486027

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] words = line.split(",");

        context.write(new Text(words[1]), new Text("ratings." + words[2]));
    }
}
