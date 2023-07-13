package com.opstty.mapper;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SortHeightMapper extends Mapper<Object, Text, FloatWritable, Text> {
    private int line = 0;

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        if (line != 0) {
            String[] text_split = value.toString().split(";");
            try {
                context.write(new FloatWritable(Float.parseFloat(text_split[6])), new Text(text_split[2]));
            }
            catch (NumberFormatException ex) {}
        }
        line++;
    }
}
