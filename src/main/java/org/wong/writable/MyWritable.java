package org.wong.writable;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MyWritable implements Writable{

    private VLongWritable field1;
    private VLongWritable field2;

    public MyWritable(){
        this.set(new VLongWritable(),new VLongWritable());
    }

    public MyWritable(VLongWritable fld1,VLongWritable fld2){
        this.set(fld1,fld2);
    }


    public void set(VLongWritable fld1,VLongWritable fld2){
        if(fld1.get()<=fld2.get()){
            this.field1=fld1;
            this.field2=fld2;
        }else{
            this.field1=fld2;
            this.field2=fld1;
        }
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        field1.write(dataOutput);
        field2.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        field1.readFields(dataInput);
        field2.readFields(dataInput);
    }
}
