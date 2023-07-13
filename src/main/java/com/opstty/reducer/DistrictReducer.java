package com.opstty.reducer;

import org.apache.commons.io.output.NullWriter;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class DistrictReducer extends Reducer<IntWritable, NullWritable, IntWritable, NullWritable> {

    public void reduce(IntWritable key, Iterable<NullWritable> values, Context context)
            throws IOException, InterruptedException {
        context.write(key, NullWritable.get());
    }
}
