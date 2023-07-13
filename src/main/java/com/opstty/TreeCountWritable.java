package com.opstty;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TreeCountWritable implements Writable {

    Integer district;
    Integer count;

    public TreeCountWritable(Integer district, Integer count){
        this.district = district;
        this.count = count;
    }

    public TreeCountWritable(){
        this.district = 0;
        this.count = 0;
    }

    public void setDistrict (Integer district) {
        this.district = district;
    }

    public void setCount (Integer count) {
        this.count = count;
    }

    public Integer getDistrict() {
        return district;
    }

    public Integer getCount() {
        return count;
    }

    public void write (DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(district);
        dataOutput.writeInt(count);
    }

    public void readFields (DataInput dataInput) throws IOException {
        district = dataInput.readInt();
        count = dataInput.readInt();
    }

    public String toString() {
        return district + "\t" + count;
    }
}
