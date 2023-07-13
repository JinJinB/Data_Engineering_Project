package com.opstty.mapper;

import com.opstty.AgeDistrictWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class OldestTreeMapper extends Mapper<Object, Text, NullWritable, AgeDistrictWritable> {
    private int line = 0;
    private AgeDistrictWritable outPut= new AgeDistrictWritable();

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        if (line != 0) {
            String[] line_split = value.toString().split(";");
            try {
                outPut.setAge(2023 - Integer.parseInt(line_split[5]));
                outPut.setDistrict(Integer.parseInt(line_split[1]));
                context.write(NullWritable.get(), outPut);
            }
            catch (NumberFormatException e) {}
        }
        line ++;
    }
}
