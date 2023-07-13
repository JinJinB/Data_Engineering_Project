package com.opstty.mapper;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class MaxHeightMapper extends Mapper<Object, Text, Text, FloatWritable> {
    private int line = 0;

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        if (line != 0) {
            String[] text_split = value.toString().split(";");
            try {
                context.write(new Text(text_split[2]), new FloatWritable(Float.parseFloat(text_split[6])));
            }
            catch (NumberFormatException ex) {}
        }
        line++;
    }
}
