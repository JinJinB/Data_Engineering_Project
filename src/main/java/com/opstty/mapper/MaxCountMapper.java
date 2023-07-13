package com.opstty.mapper;

import com.opstty.TreeCountWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MaxCountMapper extends Mapper<Object, Text, NullWritable, TreeCountWritable> {

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        String[] text_split = value.toString().split("\t");
        int district = Integer.parseInt(text_split[0]);
        int count = Integer.parseInt(text_split[1]);
        context.write(NullWritable.get(), new TreeCountWritable(district, count));
    }
}
