package com.opstty.reducer;

import com.opstty.Informations;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class OldestTreeReducer extends Reducer<IntWritable, Informations, IntWritable, NullWritable> {

    public void reduce(IntWritable dist, Iterable<Informations> values, Context context) throws IOException, InterruptedException {

        IntWritable districts = new IntWritable();

        int district_old = 0;
        int min_age = 2020;
        // find minimum plantation date
        for (Informations val : values){
            if (val.getAge().get() < min_age) {
                min_age = val.getAge().get();
                district_old = val.getDistrict().get();
            }
        }
        districts.set(district_old);
        context.write(districts,NullWritable.get());
    }
}
