package com.opstty.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SpeciesCountMapper extends Mapper<Object, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private int line = 0;

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        if (line != 0) {
            context.write(new Text(value.toString().split(";")[2]), one);
        }
        line++;
    }
}
