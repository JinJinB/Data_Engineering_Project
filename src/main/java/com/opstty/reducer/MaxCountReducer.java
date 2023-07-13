package com.opstty.reducer;

import com.opstty.TreeCountWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MaxCountReducer extends Reducer<NullWritable, TreeCountWritable, NullWritable, TreeCountWritable> {

    private final TreeCountWritable final_result = new TreeCountWritable();

    public void reduce(NullWritable key, Iterable<TreeCountWritable> values, Context context)
            throws IOException, InterruptedException {

        Integer district = 0;
        Integer count = 0;

        for (TreeCountWritable val : values) {
            district = val.getDistrict();
            count = val.getCount();

            if (count >= final_result.getCount()){
                final_result.setDistrict(district);
                final_result.setCount(count);
            }
        }

        context.write(NullWritable.get(), final_result);

    }
}
