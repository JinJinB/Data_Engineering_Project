package com.opstty.reducer;

import com.opstty.AgeDistrictWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class OldestTreeReducer extends Reducer<NullWritable, AgeDistrictWritable, IntWritable, NullWritable> {

    private AgeDistrictWritable final_result = new AgeDistrictWritable();

    public void reduce(NullWritable key, Iterable<AgeDistrictWritable> values, Context context)
            throws IOException, InterruptedException {

        Integer age = 0;
        Integer district = 0;

        final_result.setAge(0);
        final_result.setDistrict(0);

        for (AgeDistrictWritable val : values) {
            age = val.getAge();
            district = val.getDistrict();

            if (age >= final_result.getAge()) {
                final_result.setAge(age);
                final_result.setDistrict(district);
            }
        }

        context.write(new IntWritable(final_result.getDistrict()), NullWritable.get());

    }
}
