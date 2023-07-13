package com.opstty.job;

import com.opstty.TreeCountWritable;
import com.opstty.mapper.MaxCountMapper;
import com.opstty.mapper.TreeCountMapper;
import com.opstty.reducer.MaxCountReducer;
import com.opstty.reducer.TreeCountReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class DistrictCount {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: districtcount <in> [<in>...] <out>");
            System.exit(2);
        }
        Job job1 = Job.getInstance(conf, "treecount");
        job1.setJarByClass(DistrictCount.class);
        job1.setMapperClass(TreeCountMapper.class);
        job1.setCombinerClass(TreeCountReducer.class);
        job1.setReducerClass(TreeCountReducer.class);
        job1.setOutputKeyClass(IntWritable.class);
        job1.setOutputValueClass(IntWritable.class);
        Path tmp = new Path("temp");
        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job1, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job1, tmp);
        job1.waitForCompletion(true);

        Configuration configuration = new Configuration();
        Job job2 = Job.getInstance(configuration, "maxcount");
        job2.setJarByClass(DistrictCount.class);
        job2.setMapperClass(MaxCountMapper.class);
        job2.setCombinerClass(MaxCountReducer.class);
        job2.setReducerClass(MaxCountReducer.class);
        job2.setOutputKeyClass(NullWritable.class);
        job2.setOutputValueClass(TreeCountWritable.class);
        FileInputFormat.addInputPath(job2, tmp);
        FileOutputFormat.setOutputPath(job2,
                new Path(otherArgs[otherArgs.length - 1]));
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}
