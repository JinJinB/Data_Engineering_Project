package com.opstty;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AgeDistrictWritable implements Writable {

    Integer age;
    Integer district;

    public AgeDistrictWritable(){
        age = 0;
        district = 0;
    }

    public void setAge (Integer age) {
        this.age = age;
    }

    public void setDistrict (Integer district) {
        this.district = district;
    }

    public Integer getAge() {
        return age;
    }

    public Integer getDistrict() {
        return district;
    }

    public void write (DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(age);
        dataOutput.writeInt(district);
    }

    public void readFields (DataInput dataInput) throws IOException {
        age = new Integer(dataInput.readInt());
        district = new Integer(dataInput.readInt());
    }

    public String toString() {
        return age + "\t" + district;
    }
}
