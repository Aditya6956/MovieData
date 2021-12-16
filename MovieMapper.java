package com.company;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MovieMapper extends Mapper<LongWritable, Text, Text, Text> {
    //movieId,title,genres
    //7,Sabrina (1995),Comedy|Romance

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] words = line.split(",");

        context.write(new Text(words[0]), new Text("movies." + words[1]));
    }
}
